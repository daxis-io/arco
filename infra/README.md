# Infrastructure

Deployment templates for Arco.

## Terraform (GCP)

```bash
cd terraform
terraform init
terraform plan -var="project_id=your-project"
terraform apply -var="project_id=your-project"
```

## Resources Created

- **Storage bucket**: Catalog metadata storage with versioning
- More resources to be added as implementation progresses

## Environments

| Environment | Purpose |
|-------------|---------|
| dev | Local development |
| staging | Pre-production testing |
| prod | Production deployment |
