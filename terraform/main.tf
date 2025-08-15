
provider "aws" {
  region = "eu-west-2"
}

#module "aegon_demo_s3" {
#  source       = "./modules/aegon_demo_s3"
#  project_name = "aegon_demo"
#}

#module "aegon_demo_firehose" {
#  source         = "./modules/aegon_demo_firehose"
#  s3_bucket_arn  = "arn:aws:s3:::aegon-demo-datalake"
#}

#output "s3_bucket_name" {
#  value = module.aegon_demo_s3.bucket_name
#}

module "aegon_demo_glue" {
  source = "./modules/aegon_demo_glue"
}