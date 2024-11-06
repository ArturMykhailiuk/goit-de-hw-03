import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_

spark = SparkSession.builder.appName("ProductAnalysis").getOrCreate()
sc = spark.sparkContext

# 1. Завантаження CSV-файлів
users = spark.read.csv("D:/GoIT/GoITDE/goit-de-hw-03/users.csv", header=True, inferSchema=True)
purchases = spark.read.csv("D:/GoIT/GoITDE/goit-de-hw-03/purchases.csv", header=True, inferSchema=True)
products = spark.read.csv("D:/GoIT/GoITDE/goit-de-hw-03/products.csv", header=True, inferSchema=True)

print("1.Кількість рядків в кожній таблиці до очищення")
print("users: ", users.count())
print("purchases: ", purchases.count())
print("products: ", products.count())

# print("1.Вміст завантажених даних")
# users.show(n=users.count(), truncate=False)
# purchases.show(n=purchases.count(), truncate=False)
# products.show(n=products.count(), truncate=False)


# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями
users = users.dropna()
purchases = purchases.dropna()
products = products.dropna()

# print("2.Вміст очищених даних")
# users.show(n=users.count(), truncate=False)
# purchases.show(n=purchases.count(), truncate=False)
# products.show(n=products.count(), truncate=False)

print("2.Кількість рядків в кожній таблиці після очищення")
print("users: ", users.count())
print("purchases: ", purchases.count())
print("products: ", products.count())


# 3. Визначте загальну суму покупок за кожною категорією продуктів
# Об'єднання таблиць purchases і products
purchases__products = purchases.join(products, "product_id")
# Обчислення загальної суми покупок за кожною категорією продуктів
totalAmount_by_category = purchases__products.groupBy("category").agg(sum_("price").alias("total"))
print("3. Загальна сума покупок за кожною категорією продуктів")
totalAmount_by_category.show()


# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно
# Об'єднання таблиць users і purchases
users__purchases = users.join(purchases, "user_id")
# Фільтрація за віковою категорією від 18 до 25 років
age_filtered_purchases = users__purchases.filter((users__purchases["age"] >= 18) & (users__purchases["age"] <= 25))
# Об'єднання з таблицею products
age_filtered_purchases__products = age_filtered_purchases.join(products, "product_id")
# Обчислення суми покупок за кожною категорією продуктів для вікової категорії від 18 до 25 років
age_totalAmount_by_category = age_filtered_purchases__products.groupBy("category").agg(sum_("price").alias("totalAmount"))
print("4. Cума покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно")
age_totalAmount_by_category.show()


# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років
# Обчислення загальної суми покупок для вікової категорії від 18 до 25 років
age_totalAmount = age_totalAmount_by_category.agg(sum_("totalAmount").alias("totalAmount")).collect()[0]["totalAmount"]
# Обчислення частки покупок за кожною категорією
category_per_totalAmount = age_totalAmount_by_category.withColumn("percentage", age_totalAmount_by_category["totalAmount"] / age_totalAmount * 100)
print("5. Частка покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років")
category_per_totalAmount.show()


# 6. Виберіть три категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років
top_3_categories = category_per_totalAmount.orderBy(category_per_totalAmount["percentage"].desc()).limit(3)
print("6. Три категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років")
top_3_categories.show()


spark.stop()

