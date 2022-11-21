# Demo operator-kafka 

## Init controller

```
operator-sdk init --domain example.com --repo github.com/jmlero/operator-kafka --plugins=go/v4-alpha
```

## Create controller

```
operator-sdk create api --group cache --version v1alpha1 --kind KafkaTopic --resource --controller
```

## Create manifests

```
make manifests
```

## Install manifests

```
make install
```

## Build and push image

```
make docker-build docker-push IMG="jmlero/kafkatopic:v0.0.1"
```

## Deploy image

```
make deploy IMG="jmlero/kafkatopic:v0.0.1"
```

