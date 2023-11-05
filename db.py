import psycopg2
from psycopg2 import sql, OperationalError
import json
from datetime import datetime
import os
from dotenv import load_dotenv


class Database:
    def __init__(self):
        load_dotenv()
        self._connection_parameters = {
            "dbname": os.getenv('DATABASE_NAME', 'postgres'),
            "user": os.getenv('DATABASE_USER', 'postgres'),
            "password": os.getenv('DATABASE_PASSWORD'),
            "host": os.getenv('DATABASE_HOST', 'localhost')
        }
        self._create_products_table()

    def _connect(self):
        return psycopg2.connect(**self._connection_parameters)

    def _create_products_table(self):
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    CREATE TABLE IF NOT EXISTS uzum_products (
                        product_id INT NOT NULL,
                        product_title TEXT,
                        product_rating NUMERIC,
                        product_reviews_amount INT,
                        product_orders_amount INT,
                        product_real_orders_amount INT,
                        product_available_amount INT,
                        sku_id INT NOT NULL,
                        sku_available_amount INT,
                        sku_full_price INT,
                        sku_purchase_price INT,
                        sku_barcode BIGINT,
                        timestamp TIMESTAMP,
                        PRIMARY KEY (product_id, sku_id));
                    """)
        except OperationalError as _e:
            print("OperationalError: ", _e)
        finally:
            conn.close()

    def _insert_data(self, params):
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    columns = sql.SQL(', ').join(map(sql.Identifier, params.keys()))
                    values_placeholders = sql.SQL(', ').join(map(sql.Placeholder, params.keys()))

                    query = sql.SQL("""
                            INSERT INTO uzum_products ({})
                            VALUES ({})
                            ON CONFLICT (product_id, sku_id) DO NOTHING;
                        """).format(columns, values_placeholders)

                    cur.execute(query, params)
        except OperationalError as _e:
            print("OperationalError: ", _e)
        finally:
            conn.close()

    def extract_and_insert_data(self, response):
        try:
            json_data = response.json()
            if json_data.get("payload") is None:
                # print("No product here")
                return

            product_data = json_data['payload']['data']
            skus_data = product_data['skuList']
            for sku in skus_data:
                product_info = {
                    'product_id': product_data['id'],
                    'product_title': product_data['title'],
                    'product_rating': product_data['rating'],
                    'product_reviews_amount': product_data['reviewsAmount'],
                    'product_orders_amount': product_data['ordersAmount'],
                    'product_real_orders_amount': product_data['rOrdersAmount'],
                    'product_available_amount': product_data['totalAvailableAmount'],
                    'sku_id': sku['id'],
                    'sku_available_amount': sku['availableAmount'],
                    'sku_full_price': sku['fullPrice'],
                    'sku_purchase_price': sku['purchasePrice'],
                    'sku_barcode': sku['barcode'],
                    'timestamp': json_data['timestamp']
                }
                self._insert_data(product_info)

        except json.JSONDecodeError:
            print('Cannot decode JSON')
            self._save_json_for_review(response.text)

        except KeyError as e:
            print(f"Missing key in JSON data: {e}")
            self._save_json_for_review(response.text)

    @staticmethod
    def _save_json_for_review(json_str):
        filename = f"json_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            f.write(json_str)
        print(f"Saved JSON data to {filename} for review.")
