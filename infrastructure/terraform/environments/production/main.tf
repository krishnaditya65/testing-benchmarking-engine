# Production account bootstrap. Sized for protocol-load-worker-go's full
# HPA ceiling (500 pods on active-virtual-users) and multi-AZ resilience for
# the control-plane node group (Temporal/ClickHouse/Redpanda must not go
# down with a single AZ).

terraform {
  required_version = ">= 1.7.0"

  backend "s3" {
    bucket         = "testforge-production-terraform-state"
    key            = "environments/production/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "testforge-production-terraform-state-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
}

module "cloud_account_bootstrap" {
  source = "../../modules/cloud-account-bootstrap"

  environment = "production"
  aws_region  = "us-east-1"

  vpc_cidr_block          = "10.30.0.0/16"
  availability_zone_count = 3

  execution_fleet_node_instance_types = ["c6in.2xlarge", "c6in.4xlarge", "c6in.8xlarge"]
  execution_fleet_node_min_size       = 4
  execution_fleet_node_max_size       = 60
  execution_fleet_spot_enabled        = true

  control_plane_node_instance_types = ["m6i.xlarge", "m6i.2xlarge"]
  control_plane_node_min_size        = 3
  control_plane_node_max_size        = 12

  terraform_state_bucket_force_destroy = false

  tags = {
    "testforge.io/cost-center" = "platform-eng-production"
  }
}

output "eks_cluster_name" {
  value = module.cloud_account_bootstrap.eks_cluster_name
}

output "eks_cluster_endpoint" {
  value = module.cloud_account_bootstrap.eks_cluster_endpoint
}

output "execution_fleet_workload_role_arn" {
  value = module.cloud_account_bootstrap.execution_fleet_workload_role_arn
}
