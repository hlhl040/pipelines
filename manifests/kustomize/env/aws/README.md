# Kubeflow Pipelines on Amazon RDS with IAM database authentication

This overlay runs Kubeflow Pipelines against an external Amazon RDS or Aurora
MySQL database, with the API server and cache server authenticating using
short-lived IAM tokens instead of a stored password.

Each component signs a fifteen-minute token with its own IAM identity and
presents it as the MySQL password over TLS. Nothing durable is stored, the
credential expires on its own, and database access is attributable per
component.

## What this overlay does not do

**The metadata server still uses a password.** `metadata-grpc-server` runs the
upstream `google/ml-metadata` image, which reads its credentials once at startup
and cannot accept a rotating token. It will connect to RDS with the username and
password in `mysql-secret`, and because that image exposes no TLS options for
MySQL, **that connection is unencrypted**. Keep this in mind when deciding
whether the database may be reached over a private subnet only.

Until that is addressed, `mysql-secret` must keep a working password and the
corresponding native MySQL user must continue to exist.

## Prerequisites

None of these are created by this overlay.

### 1. The database

Enable IAM authentication on the cluster, then create the schemas and the
database users. Connect as the master user:

```sql
CREATE DATABASE mlpipeline;
CREATE DATABASE cachedb;
CREATE DATABASE metadb;

-- IAM-authenticated users. They are granted no CREATE DATABASE privilege,
-- the schemas above are created by the operator, not by Kubeflow Pipelines.
CREATE USER 'kfp_apiserver'@'%' IDENTIFIED WITH AWS_AUTHENTICATION_PLUGIN AS 'RDS';
GRANT ALL PRIVILEGES ON mlpipeline.* TO 'kfp_apiserver'@'%';

CREATE USER 'kfp_cache'@'%' IDENTIFIED WITH AWS_AUTHENTICATION_PLUGIN AS 'RDS';
GRANT ALL PRIVILEGES ON cachedb.* TO 'kfp_cache'@'%';

-- The metadata server, which cannot use IAM authentication yet.
CREATE USER 'kfp_metadata'@'%' IDENTIFIED BY '<password>';
GRANT ALL PRIVILEGES ON metadb.* TO 'kfp_metadata'@'%';

FLUSH PRIVILEGES;
```

Separate users keep the per-component attribution that IAM authentication buys.
A single shared user works but gives that up.

### 2. IAM policy and roles

Grant `rds-db:connect` for one database user, scoped to the cluster's resource
id — not the instance identifier, and not a wildcard:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "rds-db:connect",
    "Resource": "arn:aws:rds-db:<region>:<account>:dbuser:<cluster-resource-id>/kfp_apiserver"
  }]
}
```

Create one role per component with this policy, each trusted by the matching
Kubernetes service account through IRSA or EKS Pod Identity:

| Service account | Database user |
| --- | --- |
| `ml-pipeline` | `kfp_apiserver` |
| `kubeflow-pipelines-cache` | `kfp_cache` |

### 3. The database CA bundle

The components refuse to start with IAM authentication enabled and no CA
bundle, because the token is sent using the cleartext password plugin. Create
the ConfigMap the deployments mount:

```bash
curl -o global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
kubectl create configmap db-ca-bundle -n kubeflow --from-file=ca.pem=global-bundle.pem
```

## Configure and install

Three files need your values:

| File | What to change |
| --- | --- |
| `params.env` | The RDS endpoint, port and region |
| `db-users-secret.yaml` | The IAM database user for each component |
| `patches/*-sa.yaml` | The account id and role name for each service account |

`mysql-secret` is left alone: its username and password still belong to the
metadata server.

Then:

```bash
kubectl apply -k manifests/kustomize/env/aws
```

## Verify

```bash
kubectl -n kubeflow logs deploy/ml-pipeline | grep -i "access denied\|token"
kubectl -n kubeflow logs deploy/cache-server | grep -i "access denied\|token"
```

Then upload and run a pipeline, and confirm a second run of the same pipeline
hits the cache.

To confirm tokens are minted per connection rather than once at startup, leave
the deployment idle for more than fifteen minutes — past the token lifetime —
and then run a pipeline. `ConMaxLifeTime` defaults to `120s`, so every pooled
connection will have been recycled by then and each new one needs a fresh
token.

## Roll back

Set `dbCredentialProviderEnabled=false` in `params.env`, reapply, and restart
the deployments. The provider settings can stay in place. The components return to authenticating with the password in
`mysql-secret`.

Keep the native MySQL users and their passwords until the rollout is confirmed.
Removing them early removes this path.
