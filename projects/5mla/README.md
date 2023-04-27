# MLflow

## Исполняемый скрипт

Ключевые строки:
```python
with mlflow.start_run():
    model.fit(X_train, y_train)

print(f"fit completed")

model_score = log_loss(y_test, model.predict(X_test))

print(f"log_loss on validation: {model_score:.3f}")

mlflow.sklearn.log_model(model, artifact_path='model')
mlflow.log_metric("log_loss", model_score)
mlflow.log_param("model_param1", float(sys.argv[2]))
```

Конфигурация запуска этого скрипта средствами MLflow описана в файле MLProject.

## Tracking
Запустить MLflow tracking server (из корневой папки `~`):
```bash
conda activate dsenv
mlflow server \
--backend-store-uri sqlite:///mlruns.db \
--default-artifact-root ./mlruns \
--host 0.0.0.0:6047
```

Если не дает порт, то с помощью `ps aux | grep 6047` посмотреть все созданные PIDы и удалить их с помощью `kill -9 <PID>`.

**Важно:** перед запуском сервера желательно удалять старую версию `mlruns.db`

## Train

Запустить в новом окне терминала (из папки проекта `5mla`):
```bash
conda activate dsenv
export MLFLOW_TRACKING_URI=http://localhost:6047
cd ai-masters-bigdata/projects/5mla/
mlflow run . -P train_path=/home/users/datasets/criteo/criteo_train1 -P tol=0.1
```

## Инференс

Запустить в новом окне терминала (из папки проекта `5mla`):
```bash
mlflow models serve -p 7047 -m ./mlruns/0/2183865bad9a407c8deca7496a0945d6/artifacts/model
```