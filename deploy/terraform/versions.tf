###############################################################################
# Provider & Terraform version constraints
#
# Pinned to AWS provider 5.x: the IRSA / OIDC / EKS encryption_config argument
# names used in this module set are stable in 5.x. The `tls` provider is used
# only to compute the OIDC issuer cert thumbprint for aws_iam_openid_connect_provider.
###############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.40.0, < 6.0.0"
    }

    # Used to derive the SHA-1 thumbprint of the EKS OIDC issuer's TLS chain,
    # which aws_iam_openid_connect_provider requires (or validates against).
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0.0"
    }
  }
}
