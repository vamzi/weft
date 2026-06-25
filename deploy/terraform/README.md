# Weft AWS infrastructure (Terraform)

Provisions the single-account, self-hosted platform: **VPC** (private subnets for EKS + RDS, public
for the ALB), **EKS** cluster + node groups (with Karpenter for fast worker scale-up), **ECR**
repos, **RDS Postgres** (the `weft-meta` metastore), **ALB** + ACM TLS, the **S3 workspace bucket**,
the **EKS OIDC provider**, and the **per-cluster IAM roles** consumed via IRSA for scoped S3 access.

Outputs feed `helm install weft deploy/helm/weft` (RDS endpoint, bucket name, OIDC issuer, role
ARNs). Modules land with the Wave-1 infra agent; see `docs/deployment.md` for the user flow.
