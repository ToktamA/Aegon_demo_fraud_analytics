terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region = var.region
}

# ---------- Variables ----------
variable "region"         { type = string  default = "eu-west-2" } # where API/Lambda run
variable "bedrock_region" { type = string  default = "eu-west-1" } # where Bedrock model lives
variable "model_id" {
  type    = string
  default = "anthropic.claude-3-haiku-20240307"
}
variable "max_tokens" { type = number default = 300 }
variable "max_req"    { type = number default = 50 } # soft cap per warm container

# ---------- Lambda code (inline) ----------
locals {
  lambda_code = <<'PY'
import json, os, boto3, html

MODEL_ID = os.environ.get("MODEL_ID", "anthropic.claude-3-haiku-20240307")
REGION   = os.environ.get("BEDROCK_REGION", "us-east-1")
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "300"))
TEMP = 0.2
MAX_REQ = int(os.environ.get("MAX_REQ", "50"))

br = boto3.client("bedrock-runtime", region_name=REGION)
_REQS = 0

SYSTEM = ("You are a concise fraud analytics assistant. "
          "Use the provided context to ground answers. "
          "Keep answers under 120 words. Use percentages when relevant.")

def _resp(code, body, ctype="application/json"):
    return {"statusCode": code,
            "headers": {"Content-Type": ctype,
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Allow-Methods": "GET,POST,OPTIONS"},
            "body": body if isinstance(body, str) else json.dumps(body)}

def handler(event, _ctx):
    global _REQS
    http = event.get("requestContext", {}).get("http", {})
    method = http.get("method", "GET")
    path = http.get("path", "/")

    if method == "OPTIONS":
        return _resp(200, {"ok": True})

    # Serve tiny UI
    if method == "GET" and path == "/ui":
        qs = event.get("queryStringParameters") or {}
        ctx = f"Year={qs.get('year','')}; State={qs.get('state','')}; Category={qs.get('category','')}"
        page = f"""<!doctype html><meta charset="utf-8">
        <style>body{{font-family:ui-sans-serif,system-ui;margin:20px}}
        #out{{white-space:pre-wrap;border:1px solid #eee;padding:12px;border-radius:8px;margin-top:12px}}
        small{{opacity:.7}}</style>
        <h3>Fraud Assistant</h3>
        <input id="q" placeholder="Ask about fraud…" style="width:70%">
        <button onclick="ask()">Ask</button>
        <small id="ctx"> Context: {html.escape(ctx)}</small>
        <div id="out"></div>
        <script>
        const api = location.origin + "/chat";
        const ctx = "{html.escape(ctx)}";
        async function ask(){{
          const prompt = document.getElementById('q').value;
          document.getElementById('out').textContent = "Thinking…";
          const r = await fetch(api, {{method:"POST", headers:{{'Content-Type':'application/json'}},
            body: JSON.stringify({{prompt, context: ctx}})}});
          const j = await r.json();
          document.getElementById('out').textContent = j.answer || JSON.stringify(j,null,2);
        }}
        </script>"""
        return _resp(200, page, "text/html; charset=utf-8")

    # Chat endpoint
    if method == "POST" and path == "/chat":
        if _REQS >= MAX_REQ:
            return _resp(429, {"error":"demo request cap reached"})

        try:
            body = json.loads(event.get("body") or "{}")
        except Exception:
            return _resp(400, {"error":"invalid json"})

        prompt  = (body.get("prompt") or "").strip()[:2000]
        context = (body.get("context") or "").strip()[:4000]
        if not prompt:
            return _resp(400, {"error":"missing prompt"})

        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": MAX_TOKENS,
            "temperature": TEMP,
            "messages": [
                {"role": "system", "content": [{"type":"text","text": SYSTEM}]},
                {"role": "user",   "content": [{"type":"text","text": f"Context:\\n{context}\\n\\nQuestion:\\n{prompt}"}]}
            ]
        }
        out = br.invoke_model(modelId=MODEL_ID,
                              contentType="application/json",
                              accept="application/json",
                              body=json.dumps(payload))
        j = json.loads(out["body"].read())
        text = "".join([c.get("text","") for c in j.get("content", [])])[:2000]
        _REQS += 1
        return _resp(200, {"answer": text, "remaining": max(0, MAX_REQ - _REQS)})

    return _resp(404, {"error":"not found"})
PY
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/lambda.zip"
  source {
    content  = local.lambda_code
    filename = "index.py"
  }
}

# ---------- IAM for Lambda ----------
data "aws_caller_identity" "cur" {}
data "aws_region" "cur" {}

resource "aws_iam_role" "lambda_role" {
  name               = "fraud-llm-micro-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_policy" "bedrock_invoke" {
  name   = "fraud-llm-bedrock-invoke"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["bedrock:InvokeModel"],
      Resource = "*"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "bedrock_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.bedrock_invoke.arn
}

# ---------- Lambda ----------
resource "aws_lambda_function" "chat" {
  function_name    = "fraud-llm-micro"
  role             = aws_iam_role.lambda_role.arn
  handler          = "index.handler"
  runtime          = "python3.11"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  memory_size      = 256
  timeout          = 15
  environment {
    variables = {
      MODEL_ID       = var.model_id
      BEDROCK_REGION = var.bedrock_region
      MAX_TOKENS     = tostring(var.max_tokens)
      MAX_REQ        = tostring(var.max_req)
    }
  }
}

# ---------- API Gateway HTTP API ----------
resource "aws_apigatewayv2_api" "http" {
  name          = "fraud-llm-micro"
  protocol_type = "HTTP"
  cors_configuration {
    allow_origins = ["*"] # tighten later to your domains
    allow_methods = ["GET","POST","OPTIONS"]
    allow_headers = ["*"]
  }
}

resource "aws_apigatewayv2_integration" "lambda" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.chat.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "get_ui" {
  api_id    = aws_apigatewayv2_api.http.id
  route_key = "GET /ui"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

resource "aws_apigatewayv2_route" "post_chat" {
  api_id    = aws_apigatewayv2_api.http.id
  route_key = "POST /chat"
  target    = "integrations/${aws_apigatewayv2_integration.lambda.id}"
}

# Default stage (no '/prod' in URL)
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "$default"
  auto_deploy = true
}

# Allow API Gateway to invoke Lambda
resource "aws_lambda_permission" "apigw_invoke" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.chat.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "arn:aws:execute-api:${data.aws_region.cur.name}:${data.aws_caller_identity.cur.account_id}:${aws_apigatewayv2_api.http.id}/*/*/*"
}

# ---------- Outputs ----------
output "api_base_url" {
  value = aws_apigatewayv2_api.http.api_endpoint
}
output "ui_url" {
  value = "${aws_apigatewayv2_api.http.api_endpoint}/ui"
}
