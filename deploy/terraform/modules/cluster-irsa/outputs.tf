###############################################################################
# modules/cluster-irsa — outputs
###############################################################################

output "role_arn" {
  description = "ARN of the scoped IRSA role. Annotate the pod ServiceAccount with eks.amazonaws.com/role-arn = <this>."
  value       = aws_iam_role.this.arn
}

output "role_name" {
  description = "Name of the scoped IRSA role."
  value       = aws_iam_role.this.name
}

output "service_account_annotation" {
  description = "The exact ServiceAccount annotation map to apply: {eks.amazonaws.com/role-arn = <role arn>}."
  value       = { "eks.amazonaws.com/role-arn" = aws_iam_role.this.arn }
}
