#used for building claster for kafka 

resource "aws_msk_serverless_cluster" "paysim_fraud_pipeline" {
    cluster_name = "paysim-fraud-pipeline"
    client_authentication {
        sasl{
            iam{
                enabled = true
            }
        }
    }

    vpc_config {
        subnet_ids          = aws_subnet.private[*].id
        security_group_ids = [aws_security_group.msk_emr.id]
    }
    tags = {
        Name        = "paysim-fraud-pipeline-msk"
        Environment = var.environment
    }


}

output "msk_cluster_arn" {
    value = aws_msk_serverless_cluster.paysim_fraud_pipeline.arn
}

output "msk_bootstrap_brokers_sasl_iam" {
    value = aws_msk_serverless_cluster.paysim_fraud_pipeline.bootstrap_brokers_sasl_iam
}