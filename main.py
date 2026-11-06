from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, round

spark = SparkSession.builder \
    .appName("goit-de-hw-03") \
    .getOrCreate()

# 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.
users = spark.read.csv("data/users.csv", header=True, inferSchema=True)
purchases = spark.read.csv("data/purchases.csv", header=True, inferSchema=True)
products = spark.read.csv("data/products.csv", header=True, inferSchema=True)

# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.
users_clean = users.dropna()
purchases_clean = purchases.dropna()
products_clean = products.dropna()

# очищуємо і покупки без користувачів
joined_cleaned_df = (purchases_clean
 .join(products_clean, purchases_clean.product_id == products_clean.product_id, "inner")
  .join(users_clean, purchases_clean.user_id == users_clean.user_id, "inner")
.withColumn("total", col("quantity") * col("price")))

joined_cleaned_df.cache()

# 3. Визначте загальну суму покупок за кожною категорією продуктів.
sum_by_cat = (joined_cleaned_df
 .groupBy(col("category"))
 .agg(round(sum("total"), 2).alias("total"))
)
print("Сума покупок за кожною категорією продуктів")
sum_by_cat.show(truncate=False)


# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.
sum_by_cat_18_25 = (joined_cleaned_df.filter(col("age").between(18, 25))
    .groupBy("category")
    .agg(round(sum("total"), 2).alias("total_18_25")))

sum_by_cat_18_25.cache()
print("Сума покупок для вікової групи 18-25 років")
sum_by_cat_18_25.show(truncate=False)

# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.
total_spent = sum_by_cat_18_25.select(sum("total_18_25")).first()[0]

cat_percents_18_25 = sum_by_cat_18_25.withColumn(
    "part",
    round((col("total_18_25") / total_spent) * 100, 2)
)

print("Частка покупок для вікової групи 18-25 років")
cat_percents_18_25.show(truncate=False)

# 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.
top_3_cat = (cat_percents_18_25
    .orderBy(col("part").desc())
    .limit(3)
    .select("category"))

print("Топ-3 категорії для 18-25 років")
top_3_cat.show(truncate=False)

spark.stop()