# Staging account bootstrap. Sized to run real (but not full-scale)
# checkout-load-test.yaml-style pipelines: a fraction of production's
# execution-fleet ceiling, spot-only, single control-plane on-demand
# instance type.

terraform {
  required_version = ">= 1.7.0"

  backend "s3" {
    bucket         = "testforge-staging-terraform-state"
    key            = "environments/staging/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "testforge-staging-terraform-state-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}

module "cloud_account_bootstrap" {
  source = "../../modules/cloud-account-bootstrap"

  environment = "staging"
  aws_region  = "us-east-1"

  vpc_cidr_block          = "10.20.0.0/16"
  availability_zone_count = 2

  execution_fleet_node_instance_types = ["c6in.xlarge"]
  execution_fleet_node_min_size       = 1
  execution_fleet_node_max_size       = 8
  execution_fleet_spot_enabled        = true

  control_plane_node_instance_types = ["m6i.large"]
  control_plane_node_min_size       = 2
  control_plane_node_max_size       = 4

  terraform_state_bucket_force_destroy = true

  tags = {
    "testforge.io/cost-center" = "platform-eng-staging"
  }
}

output "eks_cluster_name" {
  value = module.cloud_account_bootstrap.eks_cluster_name
}

output "eks_cluster_endpoint" {
  value = module.cloud_account_bootstrap.eks_cluster_endpoint
}
