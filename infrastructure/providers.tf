terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.43.0"
    }
  }

  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
}
