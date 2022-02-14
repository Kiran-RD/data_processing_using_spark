sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
--username root \
--password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--table runners \
--columns 'runner_id,registration_date' \
-m 1 \
--target-dir /user/hive/warehouse/pizza_runners.db/runners \
--delete-target-dir

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
--username root \
--password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--table pizza_names \
--columns 'pizza_id,pizza_name' \
-m 1 \
--target-dir /user/hive/warehouse/pizza_runners.db/pizza_names \
--delete-target-dir

# sqoop import \
# --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
# --username root \
# --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
# --table pizza_recipes \
# --columns 'pizza_id,toppings' \
# -m 1 \
# --target-dir /user/hive/warehouse/pizza_runners.db/pizza_recipes \
# --delete-target-dir

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
--username root \
--password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--table pizza_toppings \
--columns 'topping_id,topping_name' \
-m 1 \
--target-dir /user/hive/warehouse/pizza_runners.db/pizza_toppings \
--delete-target-dir

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
--username root \
--password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--table runner_orders \
--columns 'order_id,runner_id,pickup_time,distance,duration,cancellation' \
-m 1 \
--target-dir /user/hive/warehouse/pizza_runners.db/runner_orders \
--delete-target-dir

# sqoop import \
# --connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
# --username root \
# --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
# --table customer_orders \
# --columns 'order_id,customer_id,pizza_id,exclusions,extras,order_time' \
# -m 1 \
# --target-dir /user/hive/warehouse/pizza_runners.db/customer_orders \
# --delete-target-dir

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
--username root \
--password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--query "select order_id,customer_id,pizza_id,replace(exclusions,',','|') exclusions,replace(extras,',','|') extras,order_time from customer_orders where \$CONDITIONS" \
-m 1 \
--target-dir /user/hive/warehouse/pizza_runners.db/customer_orders \
--delete-target-dir

sqoop import \
--connect jdbc:mysql://localhost:3306/pizza_runners?useSSL=False \
--username root \
--password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--query "select pizza_id,replace(toppings,',','|') from pizza_recipes where \$CONDITIONS" \
-m 1 \
--target-dir /user/hive/warehouse/pizza_runners.db/pizza_recipes \
--delete-target-dir