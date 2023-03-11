# Hadoop Streaming

## Задача

Задача предсказания вероятности клика на рекламный баннер [Criteo Dataset](https://www.kaggle.com/c/criteo-display-ad-challenge). Обучение происходит на малом датасете локальной машине. Фильтрация тестовой выборки и предсказание --- распределённо с помощью [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html).

## Dataset

Пути к выборкам:
- обучающая: local `/home/users/datasets/criteo/criteo_train1`, 1.3G 
- тестовая: hdfs `/datasets/criteo/criteo_valid_large_features`, 19.4G
- метки тестовой: local `/home/users/datasets/criteo/criteo_valid_large_filtered_labels`,  81M

## Решение

### Модель

Скрипт `model.py` содержит список признаков датасета `fields` и саму модель `model`:
```
from model import fields, model
print(type(fields))
print(type(model))
```

### Обучение

Скрипт `train.py` выделяет из тренировочной выборки валидационную. Чтобы обучить модель и вывести log loss:
```
train.sh 1 /home/users/datasets/criteo/criteo_train1
```

Аргументы:
- номер проекта
- путь к датасету

### Hadoop Streaming: предсказание

Скрипт `predict.py` можно использовать для распределённого предсказания. Для этого запускается маппер. Скрипт `predict.sh` определяет MapReduce задачу с помощью hadoop streaming:
```
predict.sh 1.joblib,predict.py, /datasets/criteo/criteo_valid_large_features predicted predict.py
```

Аргументы:
- файлы, который нужно отправить к данным
    - `1.joblib` --- обученная модель
    - `predict.py` --- скрипт, делающий предсказания
- путь к тестовой выборке
- путь для сохранения предсказаний
- маппер

### Hadoop Streaming: фильтрация

Скрипт `filter_cond.py` определяет функцию `filter_cond()`, которая принимает словарь со значениями одной записи датасета и возвращает `True`, если запись проходит через фильтр, `False` иначе.

Фильтрацию реализует скрипт `filter.py` с помощью Hadoop Streaming в виде MapReduce задачи. Запустить распределённую фильтрацию:
```
filter.sh filter.py,filter_cond.py,model.py /datasets/criteo/criteo_valid_large_features filtered filter.py
```

Аргументы:
- файлы, которые нужно отправить к данным
    - `filter.py` --- фильтрация
    - `filter_cond.py` -- условие фильтрации
    - `model.py` --- скрипт с определением имен полей
- путь к тестовой выборке
- путь для сохранения предсказаний
- маппер

### Hadoop Streaming: фильтрация + предсказание

Предыдущие два этапа легче всего совместить в виде одной MapReduce задачи, где маппер делает фильтрацию, редюсер --- предсказания. Вызывает скрипт:

```
filter_predict.sh filter.py,filter_cond.py,predict.py,model.py,1.joblib /datasets/criteo/criteo_valid_large_features pred_with_filter filter.py predict.py
```

Аргументы:
- файлы, которые нужно отправить к данным
- путь к тестовой выборке
- путь для сохранения предсказаний
- маппер
- редюсер

### Метрика

На отфильтрованных объектах считается logloss:

```
scorer_local.py /home/users/datasets/criteo/criteo_valid_large_filtered_labels pred_with_filter
```

Аргументы:
- путь к истинным меткам
- путь к предсказанным вероятностям
