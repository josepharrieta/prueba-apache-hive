#!/usr/bin/env python3
"""
Generador de datasets sintéticos para prueba de concepto de Apache Hive
Este script genera datos de e-commerce para demostrar capacidades de Hive
"""

import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import os

# Configurar Faker
fake = Faker(['es_ES', 'en_US'])

# Directorios de salida
DATASETS_DIR = '/datasets'
os.makedirs(DATASETS_DIR, exist_ok=True)

def generate_customers(num_records=10000):
    """Genera datos de clientes"""
    print(f"Generando {num_records} registros de clientes...")
    
    customers = []
    for i in range(1, num_records + 1):
        customer = {
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'country': fake.country(),
            'registration_date': fake.date_between(start_date='-5y', end_date='today'),
            'birth_date': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'gender': random.choice(['M', 'F', 'O']),
            'customer_segment': random.choice(['Premium', 'Standard', 'Basic'])
        }
        customers.append(customer)
    
    # Escribir a CSV
    with open(f'{DATASETS_DIR}/customers.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = customers[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)
    
    print(f"✓ Archivo customers.csv generado con {len(customers)} registros")
    return customers

def generate_products(num_records=1000):
    """Genera datos de productos"""
    print(f"Generando {num_records} registros de productos...")
    
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Kitchen', 'Sports', 'Beauty', 'Automotive', 'Toys']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 'GenericBrand']
    
    products = []
    for i in range(1, num_records + 1):
        category = random.choice(categories)
        product = {
            'product_id': i,
            'product_name': f"{fake.catch_phrase()} {category}",
            'category': category,
            'brand': random.choice(brands),
            'price': round(random.uniform(5.99, 999.99), 2),
            'cost': round(random.uniform(2.99, 500.00), 2),
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'dimensions': f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 100)}",
            'in_stock': random.choice([True, False]),
            'stock_quantity': random.randint(0, 1000),
            'supplier_id': random.randint(1, 50),
            'launch_date': fake.date_between(start_date='-3y', end_date='today')
        }
        products.append(product)
    
    # Escribir a CSV
    with open(f'{DATASETS_DIR}/products.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = products[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(products)
    
    print(f"✓ Archivo products.csv generado con {len(products)} registros")
    return products

def generate_orders(customers, products, num_records=50000):
    """Genera datos de órdenes"""
    print(f"Generando {num_records} registros de órdenes...")
    
    orders = []
    order_statuses = ['completed', 'pending', 'cancelled', 'refunded', 'shipped']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash']
    
    for i in range(1, num_records + 1):
        order_date = fake.date_between(start_date='-2y', end_date='today')
        
        order = {
            'order_id': i,
            'customer_id': random.choice(customers)['customer_id'],
            'order_date': order_date,
            'order_status': random.choice(order_statuses),
            'payment_method': random.choice(payment_methods),
            'shipping_cost': round(random.uniform(0, 25.99), 2),
            'tax_amount': round(random.uniform(0, 50.00), 2),
            'discount_amount': round(random.uniform(0, 100.00), 2),
            'total_amount': round(random.uniform(10.00, 2000.00), 2),
            'shipping_address': fake.address().replace('\n', ', '),
            'delivery_date': order_date + timedelta(days=random.randint(1, 30)) if random.choice([True, False]) else None,
            'notes': fake.text(max_nb_chars=100) if random.choice([True, False]) else None
        }
        orders.append(order)
    
    # Escribir a CSV
    with open(f'{DATASETS_DIR}/orders.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = orders[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(orders)
    
    print(f"✓ Archivo orders.csv generado con {len(orders)} registros")
    return orders

def generate_order_items(orders, products, avg_items_per_order=2.5):
    """Genera datos de items de órdenes"""
    total_items = int(len(orders) * avg_items_per_order)
    print(f"Generando aproximadamente {total_items} registros de items de órdenes...")
    
    order_items = []
    item_id = 1
    
    for order in orders:
        # Número aleatorio de items por orden (1-6)
        num_items = random.randint(1, 6)
        
        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 5)
            unit_price = product['price']
            
            order_item = {
                'item_id': item_id,
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': round(quantity * unit_price, 2),
                'discount_applied': round(random.uniform(0, unit_price * 0.3), 2) if random.choice([True, False]) else 0
            }
            order_items.append(order_item)
            item_id += 1
    
    # Escribir a CSV
    with open(f'{DATASETS_DIR}/order_items.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = order_items[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(order_items)
    
    print(f"✓ Archivo order_items.csv generado con {len(order_items)} registros")
    return order_items

def generate_reviews(customers, products, orders, num_records=25000):
    """Genera datos de reseñas de productos"""
    print(f"Generando {num_records} registros de reseñas...")
    
    reviews = []
    
    for i in range(1, num_records + 1):
        customer = random.choice(customers)
        product = random.choice(products)
        rating = random.randint(1, 5)
        
        # Generar texto de reseña basado en rating
        if rating >= 4:
            review_text = fake.text(max_nb_chars=200)
        elif rating == 3:
            review_text = fake.text(max_nb_chars=150)
        else:
            review_text = fake.text(max_nb_chars=100)
        
        review = {
            'review_id': i,
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'rating': rating,
            'review_text': review_text,
            'review_date': fake.date_between(start_date='-1y', end_date='today'),
            'helpful_votes': random.randint(0, 50),
            'verified_purchase': random.choice([True, False])
        }
        reviews.append(review)
    
    # Escribir a CSV
    with open(f'{DATASETS_DIR}/reviews.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = reviews[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reviews)
    
    print(f"✓ Archivo reviews.csv generado con {len(reviews)} registros")
    return reviews

def main():
    """Función principal para generar todos los datasets"""
    print("=== Generador de Datasets para Apache Hive ===")
    print("Generando datasets sintéticos de e-commerce...\n")
    
    # Generar datasets
    customers = generate_customers(10000)
    products = generate_products(1000)
    orders = generate_orders(customers, products, 50000)
    order_items = generate_order_items(orders, products)
    reviews = generate_reviews(customers, products, orders, 25000)
    
    print("\n=== Resumen de Generación ===")
    print(f"✓ Clientes: {len(customers):,} registros")
    print(f"✓ Productos: {len(products):,} registros")
    print(f"✓ Órdenes: {len(orders):,} registros")
    print(f"✓ Items de órdenes: {len(order_items):,} registros")
    print(f"✓ Reseñas: {len(reviews):,} registros")
    print(f"\nTotal de registros generados: {len(customers) + len(products) + len(orders) + len(order_items) + len(reviews):,}")
    print("\n¡Datasets generados exitosamente! 🎉")
    print(f"Archivos guardados en: {DATASETS_DIR}")

if __name__ == "__main__":
    main()
