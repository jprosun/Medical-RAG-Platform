# -------------------------------
# Project / Location
# -------------------------------

variable "project_id" {
  type        = string
  description = "GCP project ID"
  default     = "medqa-student-project"
}

variable "region" {
  type        = string
  description = "GCP region (also used as cluster location for regional GKE)"
  default     = "us-central1"
}

# -------------------------------
# Cluster naming
# -------------------------------

variable "cluster_name" {
  type        = string
  description = "Name of the GKE cluster"
  default     = "gke-medqa"
}

# -------------------------------
# Node locations (zones)
# -------------------------------

variable "node_locations" {
  description = "Zones where GKE nodes will run"
  type        = list(string)
  default     = [
    "us-central1-b"
  ]
}

# "us-central1-a",
# "us-central1-c",
# -------------------------------
# Release channel
# -------------------------------

variable "release_channel" {
  type        = string
  description = "GKE release channel (RAPID, REGULAR, STABLE)"
  default     = "REGULAR"
}

# -------------------------------
# Networking
# -------------------------------

variable "network" {
  type        = string
  description = "Name of the VPC network to create/use for GKE"
  default     = "gke-medqa-vpc"
}

variable "subnetwork" {
  type        = string
  description = "Name of the subnetwork for GKE"
  default     = "gke-medqa-subnet"
}

variable "subnet_ip_cidr" {
  type        = string
  description = "CIDR range for the primary subnet used by nodes"
  default     = "10.10.0.0/20"
}

variable "pods_secondary_range_name" {
  type        = string
  description = "Secondary IP range name for Pods"
  default     = "gke-pods"
}

variable "pods_ip_cidr" {
  type        = string
  description = "CIDR range for Pods secondary range"
  default     = "10.20.0.0/16"
}

variable "services_secondary_range_name" {
  type        = string
  description = "Secondary IP range name for Services"
  default     = "gke-services"
}

variable "services_ip_cidr" {
  type        = string
  description = "CIDR range for Services secondary range"
  default     = "10.30.0.0/20"
}

# -------------------------------
# Node pool sizing / type
# -------------------------------

variable "machine_type" {
  type        = string
  description = "Machine type for GKE nodes (ELK/RAG friendly)"
  default     = "e2-standard-4" # 4 vCPU, 16GB RAM
}

variable "node_min_count" {
  type        = number
  description = "Minimum number of nodes in the primary node pool"
  default     = 1
}

variable "node_max_count" {
  type        = number
  description = "Maximum number of nodes in the primary node pool"
  default     = 2
}

variable "node_disk_size_gb" {
  type        = number
  description = "Disk size (GB) for each node, important for ELK/Prometheus"
  default     = 80
}

variable "node_disk_type" {
  type        = string
  description = "Disk type for nodes (pd-standard, pd-balanced, pd-ssd)"
  default     = "pd-balanced"
}
