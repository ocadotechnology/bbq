## Terraform setup

1. Install [Terraform](https://www.terraform.io/) if it's not available,
1. Create [Terraform backend](https://www.terraform.io/docs/backends/) GCS bucket, which will be used to store TF state:
      ```bash
      export TERRAFORM_STATE_BUCKET_ID="<your-bucket-id-to-store-terraform-state>"
      gsutil mb -p ${BBQ_PROJECT_ID} gs://${TERRAFORM_STATE_BUCKET_ID}/
      ```
1. Export BBQ, BBQ metadata and BBQ restoration project ids: 
      ```bash
      export TF_VAR_bbq_project=${BBQ_PROJECT_ID}
      export TF_VAR_bbq_metadata_project=${BBQ_METADATA_PROJECT_ID}
      export TF_VAR_bbq_restoration_project=${BBQ_RESTORATION_PROJECT_ID}
      ```
1. Create a bucket to store remotely infrastructure state [Terraform](https://www.terraform.io/) by running the following commands:
      ```bash
      terraform init -backend-config="bucket=${TERRAFORM_STATE_BUCKET_ID}"
      ```