variable "environment" {
  description = "Deployment environment this account bootstrap targets. Drives resource sizing defaults and naming suffixes across the module."
  type        = string

  validation {
    condition     = contains(["staging", "production"], var.environment)
    error_message = "environment must be one of: staging, production."
  }
}

variable "aws_region" {
  description = "AWS region the EKS cluster, VPC, and supporting resources are created in."
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr_block" {
  description = "CIDR block for the platform VPC. Must be large enough for the execution-fleet worker subnets, which scale to hundreds of pods per node during load-test bursts."
  type        = string
  default     = "10.20.0.0/16"
}

variable "availability_zone_count" {
  description = "Number of AZs to spread private/public subnets across."
  type        = number
  default     = 3
}

variable "eks_cluster_version" {
  description = "Kubernetes version for the EKS control plane."
  type        = string
  default     = "1.29"
}

variable "execution_fleet_node_instance_types" {
  description = "Instance types for the execution-fleet node group. Chosen for high sustained network throughput (protocol-load-worker-go, protocol-load-worker-rust are network-bound, not CPU-bound at moderate VU counts)."
  type        = list(string)
  default     = ["c6in.2xlarge", "c6in.4xlarge"]
}

variable "execution_fleet_node_min_size" {
  description = "Minimum node count for the execution-fleet autoscaling group. Kept above zero so a HorizontalPodAutoscaler scale-up isn't blocked on a cold node group."
  type        = number
  default     = 2
}

variable "execution_fleet_node_max_size" {
  description = "Maximum node count for the execution-fleet autoscaling group. Sized to support protocol-load-worker-go's HPA ceiling of 500 pods."
  type        = number
  default     = 60
}

variable "execution_fleet_spot_enabled" {
  description = "Whether the execution-fleet node group uses spot capacity. Safe because worker state is checkpointed to Redis before each load-phase transition (see ARCHITECTURE.md 'Durability by design')."
  type        = bool
  default     = true
}

variable "control_plane_node_instance_types" {
  description = "Instance types for the orchestration-control-plane and telemetry-lake node group. General-purpose, on-demand only — these run Temporal, ClickHouse, and Redpanda, which are not safe to preempt."
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "control_plane_node_min_size" {
  type    = number
  default = 3
}

variable "control_plane_node_max_size" {
  type    = number
  default = 12
}

variable "terraform_state_bucket_force_destroy" {
  description = "Whether the Terraform state bucket can be destroyed with objects still in it. Must stay false in production."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Common resource tags applied across every resource this module creates."
  type        = map(string)
  default     = {}
}
