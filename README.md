## Задача

Глобальная цель всего проекта - построить полную цепочку ETL-действий:

-  загрузить данные из источника (база данных и файлы);
-  сохранить их в *staging*-е (как есть) — Hive;
-  преобразовать в снежинку (в *Hive*) для BI-аналитика;
-  преобразовать в датасет для *data* *scientist'а*;
-  сохранить датасет в хранилище *Hive*;
-  автоматизировать весь процесс с помощью *Airflow*.



## Данные

Датасеты с *kaggle* по Нобелевским лауреатам и описательная информация по странам и городам):

- [Country Data](https://www.kaggle.com/timoboz/country-data);
- [Countries Of The World](https://www.kaggle.com/fernandol/countries-of-the-world);
- [World Cities Database](https://www.kaggle.com/max-mind/world-cities-database);
- [Nobel Laureates](https://www.kaggle.com/nobelfoundation/nobel-laureates).

