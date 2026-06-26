###############################################################################
# Root outputs
#
# Consumed by `helm install weft deploy/helm/weft` and the control-plane gateway:
#   - oidc_provider_arn / cluster_oidc_issuer_url : referenced when minting any
#     additional IRSA roles out-of-band.
#   - gateway_role_arn        : annotate the gateway ServiceAccount with this.
#   - cluster_irsa_role_arns  : map cluster-id -> role ARN; the gateway sets
#     eks.amazonaws.com/role-arn on each compute pod's ServiceAccount.
###############################################################################

output "cluster_name" {
  description = "EKS cluster name."
  value       = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64 cluster CA, for kubeconfig / Helm provider wiring."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_oidc_issuer_url" {
  description = "EKS OIDC issuer URL (https://...). The trust anchor for IRSA."
  value       = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

output "oidc_provider_arn" {
  description = "ARN of the IAM OIDC provider every IRSA trust policy federates against."
  value       = aws_iam_openid_connect_provider.eks.arn
}

output "secrets_kms_key_arn" {
  description = "CMK ARN encrypting Kubernetes Secrets at rest in etcd (control #4)."
  value       = aws_kms_key.eks_secrets.arn
}

output "node_launch_template_id" {
  description = "IMDSv2-hardened worker launch template (control #3). Reuse this for any additional node group / wire the same metadata_options into a Karpenter EC2NodeClass."
  value       = aws_launch_template.node.id
}

output "node_role_arn" {
  description = "Worker node IAM role ARN (the broad role that control #3 prevents pods from stealing via IMDS)."
  value       = aws_iam_role.node.arn
}

output "gateway_role_arn" {
  description = "Scoped gateway IRSA role ARN (control #2). Set as eks.amazonaws.com/role-arn on the gateway ServiceAccount."
  value       = aws_iam_role.gateway.arn
}

output "cluster_irsa_role_arns" {
  description = "Map of compute-cluster id -> scoped IRSA role ARN (control #1). The gateway annotates each compute pod's ServiceAccount with the matching value."
  value       = { for k, m in module.cluster_irsa : k => m.role_arn }
}

output "cluster_irsa_sa_annotations" {
  description = "Convenience map of compute-cluster id -> the exact {eks.amazonaws.com/role-arn = <arn>} annotation to stamp on its ServiceAccount."
  value       = { for k, m in module.cluster_irsa : k => { "eks.amazonaws.com/role-arn" = m.role_arn } }
}
