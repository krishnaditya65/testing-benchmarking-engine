# cloud-account-bootstrap
#
# The one-time-per-environment path: stands up the VPC, EKS cluster, node
# groups, IAM roles, and Terraform state bucket that everything else in this
# repo assumes already exists. This is *not* how per-pipeline-run test
# namespaces get created — that on-demand path is the Crossplane composition
# in infrastructure/crossplane/compositions/ephemeral-test-namespace-composition.yaml,
# invoked at runtime by environment-lifecycle-manager. See
# infrastructure/crossplane/README.md for the split rationale.

terraform {
  required_version = ">= 1.7.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.27"
    }
  }
}

locals {
  name_prefix = "testforge-${var.environment}"

  common_tags = merge(var.tags, {
    "testforge.io/environment"  = var.environment
    "testforge.io/managed-by"   = "terraform"
    "testforge.io/module"       = "cloud-account-bootstrap"
  })

  azs = slice(
    data.aws_availability_zones.available.names,
    0,
    var.availability_zone_count,
  )
}

data "aws_availability_zones" "available" {
  state = "available"
}

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

resource "aws_vpc" "platform" {
  cidr_block           = var.vpc_cidr_block
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-vpc"
  })
}

resource "aws_internet_gateway" "platform" {
  vpc_id = aws_vpc.platform.id

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-igw"
  })
}

# Public subnets: NAT gateways + load balancer ingress only.
resource "aws_subnet" "public" {
  count                   = var.availability_zone_count
  vpc_id                  = aws_vpc.platform.id
  cidr_block              = cidrsubnet(var.vpc_cidr_block, 4, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true

  tags = merge(local.common_tags, {
    Name                                          = "${local.name_prefix}-public-${local.azs[count.index]}"
    "kubernetes.io/role/elb"                      = "1"
    "kubernetes.io/cluster/${local.name_prefix}"  = "shared"
  })
}

# Private subnets: EKS nodes for both the execution-fleet and control-plane
# node groups live here. Kept in one CIDR range per AZ (rather than split
# per node group) to keep pod-to-pod routing simple; node-group placement
# is enforced by nodeSelector/taints, not subnet boundaries.
resource "aws_subnet" "private" {
  count             = var.availability_zone_count
  vpc_id            = aws_vpc.platform.id
  cidr_block        = cidrsubnet(var.vpc_cidr_block, 4, count.index + var.availability_zone_count)
  availability_zone = local.azs[count.index]

  tags = merge(local.common_tags, {
    Name                                          = "${local.name_prefix}-private-${local.azs[count.index]}"
    "kubernetes.io/role/internal-elb"             = "1"
    "kubernetes.io/cluster/${local.name_prefix}"  = "shared"
  })
}

resource "aws_eip" "nat" {
  count  = var.availability_zone_count
  domain = "vpc"

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-nat-eip-${count.index}"
  })
}

resource "aws_nat_gateway" "platform" {
  count         = var.availability_zone_count
  allocation_id = aws_eip.nat[count.index].id
  subnet_id     = aws_subnet.public[count.index].id

  tags = merge(local.common_tags, {
    Name = "${local.name_prefix}-nat-${count.index}"
  })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.platform.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.platform.id
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-public-rt" })
}

resource "aws_route_table" "private" {
  count  = var.availability_zone_count
  vpc_id = aws_vpc.platform.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.platform[count.index].id
  }

  tags = merge(local.common_tags, { Name = "${local.name_prefix}-private-rt-${count.index}" })
}

resource "aws_route_table_association" "public" {
  count          = var.availability_zone_count
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = var.availability_zone_count
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[count.index].id
}

# ---------------------------------------------------------------------------
# EKS control plane
# ---------------------------------------------------------------------------

resource "aws_eks_cluster" "platform" {
  name     = local.name_prefix
  role_arn = aws_iam_role.eks_cluster.arn
  version  = var.eks_cluster_version

  vpc_config {
    subnet_ids              = concat(aws_subnet.public[*].id, aws_subnet.private[*].id)
    endpoint_private_access = true
    endpoint_public_access  = true
  }

  tags = local.common_tags

  depends_on = [aws_iam_role_policy_attachment.eks_cluster_policy]
}

resource "aws_iam_role" "eks_cluster" {
  name = "${local.name_prefix}-eks-cluster-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

# ---------------------------------------------------------------------------
# Node groups: execution-fleet (spot, autoscaling, network-optimized) and
# control-plane (on-demand, stable) are separate groups so a spot
# interruption in the fleet never touches Temporal/ClickHouse/Redpanda pods.
# ---------------------------------------------------------------------------

resource "aws_eks_node_group" "execution_fleet" {
  cluster_name    = aws_eks_cluster.platform.name
  node_group_name = "${local.name_prefix}-execution-fleet"
  node_role_arn   = aws_iam_role.execution_fleet_node.arn
  subnet_ids      = aws_subnet.private[*].id

  capacity_type  = var.execution_fleet_spot_enabled ? "SPOT" : "ON_DEMAND"
  instance_types = var.execution_fleet_node_instance_types

  scaling_config {
    min_size     = var.execution_fleet_node_min_size
    max_size     = var.execution_fleet_node_max_size
    desired_size = var.execution_fleet_node_min_size
  }

  labels = {
    "testforge.io/execution-fleet" = "true"
  }

  taint {
    key    = "testforge.io/execution-fleet"
    value  = "true"
    effect = "NO_SCHEDULE"
  }

  tags = local.common_tags
}

resource "aws_eks_node_group" "control_plane" {
  cluster_name    = aws_eks_cluster.platform.name
  node_group_name = "${local.name_prefix}-control-plane"
  node_role_arn   = aws_iam_role.control_plane_node.arn
  subnet_ids      = aws_subnet.private[*].id

  capacity_type  = "ON_DEMAND"
  instance_types = var.control_plane_node_instance_types

  scaling_config {
    min_size     = var.control_plane_node_min_size
    max_size     = var.control_plane_node_max_size
    desired_size = var.control_plane_node_min_size
  }

  labels = {
    "testforge.io/control-plane" = "true"
  }

  tags = local.common_tags
}

resource "aws_iam_role" "execution_fleet_node" {
  name = "${local.name_prefix}-execution-fleet-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role" "control_plane_node" {
  name = "${local.name_prefix}-control-plane-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "execution_fleet_node_policies" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.execution_fleet_node.name
  policy_arn = each.value
}

resource "aws_iam_role_policy_attachment" "control_plane_node_policies" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.control_plane_node.name
  policy_arn = each.value
}

# ---------------------------------------------------------------------------
# IRSA role the execution-fleet workers assume: read-only access to the
# telemetry-lake ingestion topic credentials and nothing else. Workers never
# need write access to platform state — they only emit telemetry and pull
# dispatch orders.
# ---------------------------------------------------------------------------

resource "aws_iam_role" "execution_fleet_workload" {
  name = "${local.name_prefix}-protocol-load-worker-irsa"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = aws_iam_openid_connect_provider.eks.arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${replace(aws_iam_openid_connect_provider.eks.url, "https://", "")}:sub" = "system:serviceaccount:testforge-platform:protocol-load-worker-go"
        }
      }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_openid_connect_provider" "eks" {
  url = aws_eks_cluster.platform.identity[0].oidc[0].issuer

  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["9e99a48a9960b14926bb7f3b02e22da2b0ab7280"]

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# Terraform remote state bucket. Bootstrapped by this module but referenced
# by the environments/* root modules as their own backend once it exists
# (classic chicken-and-egg: first apply uses local state, then `terraform
# init -migrate-state` once this bucket is up).
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "terraform_state" {
  bucket        = "${local.name_prefix}-terraform-state"
  force_destroy = var.terraform_state_bucket_force_destroy

  tags = local.common_tags
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_dynamodb_table" "terraform_state_lock" {
  name         = "${local.name_prefix}-terraform-state-lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = local.common_tags
}
