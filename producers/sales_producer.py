import json
import uuid
import time
import random
import argparse
import signal
from datetime import datetime
from typing import Any

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel

console = Console()

# Initialize Faker with Thai locale
fake = Faker(['th_TH', 'en_US'])
Faker.seed(42)


# PRODUCT CATALOG
PRODUCTS = {
    'Electronics': [
        {'id': 'PROD-E001', 'name': 'iPhone 15 Pro', 'price': 45900.00},
        {'id': 'PROD-E002', 'name': 'Samsung Galaxy S24', 'price': 35900.00},
        {'id': 'PROD-E003', 'name': 'MacBook Pro 14"', 'price': 69900.00},
        {'id': 'PROD-E004', 'name': 'iPad Air', 'price': 24900.00},
        {'id': 'PROD-E005', 'name': 'AirPods Pro', 'price': 8990.00},
        {'id': 'PROD-E006', 'name': 'Sony WH-1000XM5', 'price': 12990.00},
        {'id': 'PROD-E007', 'name': 'Dell XPS 15', 'price': 55900.00},
        {'id': 'PROD-E008', 'name': 'Nintendo Switch', 'price': 10990.00},
    ],
    'Clothing': [
        {'id': 'PROD-C001', 'name': 'เสื้อโปโล Uniqlo', 'price': 590.00},
        {'id': 'PROD-C002', 'name': 'กางเกงยีนส์ Levi\'s', 'price': 2490.00},
        {'id': 'PROD-C003', 'name': 'รองเท้า Nike Air Max', 'price': 5490.00},
        {'id': 'PROD-C004', 'name': 'เสื้อยืด H&M', 'price': 399.00},
        {'id': 'PROD-C005', 'name': 'กระเป๋า Coach', 'price': 12900.00},
        {'id': 'PROD-C006', 'name': 'นาฬิกา Casio', 'price': 1990.00},
    ],
    'Home & Garden': [
        {'id': 'PROD-H001', 'name': 'โซฟา IKEA', 'price': 15900.00},
        {'id': 'PROD-H002', 'name': 'โต๊ะทำงาน', 'price': 4990.00},
        {'id': 'PROD-H003', 'name': 'เก้าอี้สำนักงาน', 'price': 3490.00},
        {'id': 'PROD-H004', 'name': 'ที่นอน King Size', 'price': 25900.00},
        {'id': 'PROD-H005', 'name': 'ตู้เย็น Samsung', 'price': 18900.00},
        {'id': 'PROD-H006', 'name': 'เครื่องซักผ้า LG', 'price': 12900.00},
    ],
    'Food & Beverage': [
        {'id': 'PROD-F001', 'name': 'กาแฟ Starbucks (กล่อง)', 'price': 450.00},
        {'id': 'PROD-F002', 'name': 'ช็อกโกแลต Lindt', 'price': 350.00},
        {'id': 'PROD-F003', 'name': 'ไวน์แดง', 'price': 890.00},
        {'id': 'PROD-F004', 'name': 'ชาเขียว Matcha', 'price': 290.00},
        {'id': 'PROD-F005', 'name': 'น้ำผึ้งแท้', 'price': 450.00},
    ],
    'Beauty': [
        {'id': 'PROD-B001', 'name': 'ครีมบำรุงผิว SK-II', 'price': 5990.00},
        {'id': 'PROD-B002', 'name': 'น้ำหอม Chanel', 'price': 4500.00},
        {'id': 'PROD-B003', 'name': 'ลิปสติก MAC', 'price': 890.00},
        {'id': 'PROD-B004', 'name': 'เซรั่ม Estee Lauder', 'price': 3290.00},
        {'id': 'PROD-B005', 'name': 'ครีมกันแดด Biore', 'price': 350.00},
    ],
}

REGIONS = ['Bangkok', 'Central', 'North', 'Northeast', 'South', 'East', 'West']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'cash', 'bank_transfer', 'promptpay']
ORDER_STATUSES = ['completed', 'completed', 'completed', 'pending', 'processing']  # Weighted

# Customer pool
CUSTOMERS: dict[str, dict] = {}


def get_or_create_customer(customer_id: str) -> dict:
    """Get existing customer or create new one."""
    if customer_id not in CUSTOMERS:
        CUSTOMERS[customer_id] = {
            'id': customer_id,
            'name': fake.name(),
            'email': fake.email(),
            'region': random.choice(REGIONS),
        }
    return CUSTOMERS[customer_id]


def generate_order_id() -> str:
    """Generate order ID in format ORD-YYYY-NNNNN."""
    year = datetime.now().year
    num = random.randint(10000, 99999)
    return f"ORD-{year}-{num}"


def generate_sales_order() -> dict[str, Any]:
    """Generate a realistic sales order."""
    # Select random category and product
    category = random.choice(list(PRODUCTS.keys()))
    product = random.choice(PRODUCTS[category])
    
    # Get or create customer
    customer_id = f"CUST-{random.randint(1, 500):04d}"
    customer = get_or_create_customer(customer_id)
    
    # Generate order details
    quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 30, 12, 5, 3])[0]
    unit_price = product['price']
    
    # Apply random discount (0-15%)
    discount_pct = random.choices([0, 5, 10, 15], weights=[60, 20, 15, 5])[0]
    discount_amount = (unit_price * quantity) * (discount_pct / 100)
    total_amount = (unit_price * quantity) - discount_amount
    
    # Sales rep
    sales_reps = ['พนักงาน A', 'พนักงาน B', 'พนักงาน C', 'พนักงาน D', 'พนักงาน E']
    
    return {
        'order_id': generate_order_id(),
        'customer_id': customer['id'],
        'customer_name': customer['name'],
        'customer_email': customer['email'],
        'product_id': product['id'],
        'product_name': product['name'],
        'category': category,
        'quantity': quantity,
        'unit_price': unit_price,
        'discount_percent': discount_pct,
        'total_amount': round(total_amount, 2),
        'payment_method': random.choice(PAYMENT_METHODS),
        'region': customer['region'],
        'sales_rep': random.choice(sales_reps),
        'order_status': random.choice(ORDER_STATUSES),
        'order_timestamp': fake.date_time_between(
            start_date=datetime(2026, 1, 17),
            end_date=datetime(2026, 1, 17, 23, 59, 59)
        ).isoformat() + 'Z'
    }


class SalesProducer:
    """Kafka producer for sales data."""
    
    def __init__(self, bootstrap_servers: str, rate: int):
        self.bootstrap_servers = bootstrap_servers
        self.rate = rate
        self.running = True
        
        # Metrics
        self.order_count = 0
        self.total_revenue = 0.0
        self.error_count = 0
        self.start_time = None
        
        self.producer = None
    
    def connect(self) -> bool:
        """Connect to Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.bootstrap_servers],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3,
            )
            console.print(f"[green]Connected to Kafka at {self.bootstrap_servers}[/green]")
            return True
        except KafkaError as e:
            console.print(f"[red]Failed to connect: {e}[/red]")
            return False
    
    def create_stats_table(self) -> Table:
        """Create stats display table."""
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.order_count / elapsed if elapsed > 0 else 0
        
        table = Table(title="Sales Data Producer", show_header=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        
        table.add_row("Runtime", f"{elapsed:.1f}s")
        table.add_row("Orders Generated", f"{self.order_count:,}")
        table.add_row("Total Revenue", f"฿{self.total_revenue:,.2f}")
        table.add_row("Avg Order Value", f"฿{self.total_revenue/max(self.order_count,1):,.2f}")
        table.add_row("Rate", f"{rate:.1f} orders/sec")
        table.add_row("Errors", f"{self.error_count}")
        
        return table
    
    def run(self):
        """Main producer loop."""
        if not self.connect():
            return
        
        self.start_time = time.time()
        sleep_time = 1.0 / self.rate
        
        console.print(Panel.fit(
            f"Generating sales orders at ~{self.rate} orders/sec\n"
            f"Topic: sales_orders\n"
            f"Press Ctrl+C to stop",
            title="Producer Started"
        ))
        
        try:
            with Live(self.create_stats_table(), refresh_per_second=2) as live:
                while self.running:
                    order = generate_sales_order()
                    
                    try:
                        self.producer.send('sales_orders', order)
                        self.order_count += 1
                        self.total_revenue += order['total_amount']
                    except Exception as e:
                        self.error_count += 1
                    
                    if self.order_count % 50 == 0:
                        self.producer.flush()
                    
                    live.update(self.create_stats_table())
                    time.sleep(sleep_time)
                    
        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down...[/yellow]")
        finally:
            self.producer.flush()
            self.producer.close()
            console.print(f"[green]Total orders: {self.order_count:,}, Revenue: ฿{self.total_revenue:,.2f}[/green]")
    
    def stop(self):
        self.running = False


def main():
    parser = argparse.ArgumentParser(description='Sales Data Producer')
    parser.add_argument('--rate', type=int, default=20, help='Orders per second')
    parser.add_argument('--bootstrap', type=str, default='localhost:9092')
    args = parser.parse_args()
    
    producer = SalesProducer(args.bootstrap, args.rate)
    
    def signal_handler(sig, frame):
        producer.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    producer.run()


if __name__ == '__main__':
    main()
