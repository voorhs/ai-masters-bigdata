# Hive

Задание, аналогичное [первой домашке](https://github.com/voorhs/ai-masters-bigdata/tree/main/projects/1).

## Задача

Задача предсказания вероятности клика на рекламный баннер [Criteo Dataset](https://www.kaggle.com/c/criteo-display-ad-challenge). Обучение происходит на малом датасете локальной машине. Фильтрация тестовой выборки и предсказание --- распределённо с помощью [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html).

## Dataset

Пути к выборкам:
- обучающая: local `/home/users/datasets/criteo/criteo_train1`, 1.3G 
- тестовая: hdfs `/datasets/criteo/criteo_valid_large_features`, 19.4G
- метки тестовой: local `/home/users/datasets/criteo/criteo_valid_large_filtered_labels`,  81M

## Решение

### Модель

Скрипт `model.py` содержит список признаков датасета `fields` и саму модель `model`.

### Обучение

Скрипт `train.py` выделяет из тренировочной выборки валидационную. Чтобы обучить модель и вывести log loss:
```
train.sh 2 /home/users/datasets/criteo/criteo_train1
```

Аргументы:
- номер проекта
- путь к датасету

Дополнительно в файле `2.joblib` сохраняется обученная модель.

### Hive: предсказание и фильтрация

Чтобы прогнать модель по такому же пути, как в [первой домашке](https://github.com/voorhs/ai-masters-bigdata/tree/main/projects/1), нужно запустить следующий скрипт:

```
create database if not exists ${NAME};
use ${NAME};

drop table hw2_test;
source projects/2/create_test.hql;
describe hw2_test;
select count(id) from hw2_test;

drop table hw2_pred;
source projects/2/create_pred.hql;
describe hw2_pred;

source projects/2/filter_predict.hql;
select count(id) from hw2_pred;

source projects/2/select_out.hql;
```

В результате на кластере в папке `voorhs_hiveout` сохранятся предсказания.