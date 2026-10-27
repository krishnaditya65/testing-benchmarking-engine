output "vpc_id" {
  description = "ID of the platform VPC."
  value       = aws_vpc.platform.id
}

output "private_subnet_ids" {
  description = "Private subnet IDs hosting both EKS node groups."
  value       = aws_subnet.private[*].id
}

output "public_subnet_ids" {
  description = "Public subnet IDs used for NAT gateways and ingress load balancers."
  value       = aws_subnet.public[*].id
}

output "eks_cluster_name" {
  description = "Name of the EKS cluster. Consumed by infrastructure/kubernetes and infrastructure/helm-charts deploy pipelines to target kubectl/helm context."
  value       = aws_eks_cluster.platform.name
}

output "eks_cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = aws_eks_cluster.platform.endpoint
}

output "eks_cluster_certificate_authority_data" {
  value     = aws_eks_cluster.platform.certificate_authority[0].data
  sensitive = true
}

output "eks_oidc_provider_arn" {
  description = "OIDC provider ARN used for IRSA role trust policies, including the environment-lifecycle-manager IAM role that assumes Crossplane's provider identity."
  value       = aws_iam_openid_connect_provider.eks.arn
}

output "execution_fleet_workload_role_arn" {
  description = "IAM role ARN protocol-load-worker-go pods assume via IRSA."
  value       = aws_iam_role.execution_fleet_workload.arn
}

output "terraform_state_bucket" {
  description = "S3 bucket name holding this environment's Terraform state, for backend configuration in environments/*/main.tf."
  value       = aws_s3_bucket.terraform_state.bucket
}

output "terraform_state_lock_table" {
  description = "DynamoDB table name used for Terraform state locking."
  value       = aws_dynamodb_table.terraform_state_lock.name
}
