import json
import time
import argparse
import signal
from datetime import datetime
from typing import Any

import clickhouse_connect
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel

console = Console()


class SalesConsumer:
    """Consumes sales orders from Kafka and writes to ClickHouse."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        clickhouse_host: str,
        clickhouse_port: int,
        batch_size: int
    ):
        self.bootstrap_servers = bootstrap_servers
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.batch_size = batch_size
        self.running = True
        
        # Metrics
        self.orders_consumed = 0
        self.orders_inserted = 0
        self.total_revenue = 0.0
        self.batches_written = 0
        self.error_count = 0
        self.start_time = None
        
        self.consumer = None
        self.clickhouse_client = None
        self.batch: list[tuple] = []
    
    def connect_kafka(self) -> bool:
        """Connect to Kafka."""
        try:
            self.consumer = KafkaConsumer(
                'sales_orders',
                bootstrap_servers=[self.bootstrap_servers],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='sales_clickhouse_consumer',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=500,
            )
            console.print(f"[green]Connected to Kafka at {self.bootstrap_servers}[/green]")
            return True
        except KafkaError as e:
            console.print(f"[red]Failed to connect to Kafka: {e}[/red]")
            return False
    
    def connect_clickhouse(self) -> bool:
        """Connect to ClickHouse."""
        try:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                database='analytics'
            )
            self.clickhouse_client.query('SELECT 1')
            console.print(f"[green]Connected to ClickHouse at {self.clickhouse_host}:{self.clickhouse_port}[/green]")
            return True
        except Exception as e:
            console.print(f"[red]Failed to connect to ClickHouse: {e}[/red]")
            return False
    
    def parse_order(self, order: dict[str, Any]) -> tuple:
        """Parse order into tuple for ClickHouse insert."""
        ts_str = order.get('order_timestamp', '').rstrip('Z')
        try:
            order_ts = datetime.fromisoformat(ts_str)
        except ValueError:
            order_ts = datetime.utcnow()
        
        return (
            order.get('order_id', ''),
            order.get('customer_id', ''),
            order.get('customer_name', ''),
            order.get('customer_email', ''),
            order.get('product_id', ''),
            order.get('product_name', ''),
            order.get('category', ''),
            int(order.get('quantity', 1)),
            float(order.get('unit_price', 0)),
            float(order.get('discount_percent', 0)),
            float(order.get('total_amount', 0)),
            order.get('payment_method', ''),
            order.get('region', ''),
            order.get('sales_rep', ''),
            order.get('order_status', ''),
            order_ts,
        )
    
    def flush_batch(self):
        """Write batch to ClickHouse."""
        if not self.batch:
            return
        
        try:
            self.clickhouse_client.insert(
                'sales_orders',
                self.batch,
                column_names=[
                    'order_id', 'customer_id', 'customer_name', 'customer_email',
                    'product_id', 'product_name', 'category', 'quantity',
                    'unit_price', 'discount_percent', 'total_amount',
                    'payment_method', 'region', 'sales_rep', 'order_status',
                    'order_timestamp'
                ]
            )
            self.orders_inserted += len(self.batch)
            self.batches_written += 1
            self.batch = []
        except Exception as e:
            console.print(f"[red]ClickHouse insert failed: {e}[/red]")
            self.error_count += 1
    
    def create_stats_table(self) -> Table:
        """Create stats display table."""
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.orders_inserted / elapsed if elapsed > 0 else 0
        
        table = Table(title="Sales Consumer", show_header=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green", justify="right")
        
        table.add_row("Runtime", f"{elapsed:.1f}s")
        table.add_row("Orders Consumed", f"{self.orders_consumed:,}")
        table.add_row("Orders Inserted", f"{self.orders_inserted:,}")
        table.add_row("Total Revenue", f"฿{self.total_revenue:,.2f}")
        table.add_row("Batches Written", f"{self.batches_written:,}")
        table.add_row("Batch Size", f"{len(self.batch)}/{self.batch_size}")
        table.add_row("Insert Rate", f"{rate:.1f} orders/sec")
        table.add_row("Errors", f"{self.error_count}")
        
        return table
    
    def run(self):
        """Main consumer loop."""
        if not self.connect_kafka():
            return
        if not self.connect_clickhouse():
            return
        
        self.start_time = time.time()
        last_flush_time = time.time()
        
        console.print(Panel.fit(
            f"Consuming from topic: sales_orders\n"
            f"Batch size: {self.batch_size}\n"
            f"Press Ctrl+C to stop",
            title="Consumer Started"
        ))
        
        try:
            with Live(self.create_stats_table(), refresh_per_second=2) as live:
                while self.running:
                    records = self.consumer.poll(timeout_ms=1000, max_records=100)
                    
                    for topic_partition, messages in records.items():
                        for msg in messages:
                            order = msg.value
                            self.orders_consumed += 1
                            self.total_revenue += float(order.get('total_amount', 0))
                            
                            row = self.parse_order(order)
                            self.batch.append(row)
                    
                    current_time = time.time()
                    if len(self.batch) >= self.batch_size or \
                       (self.batch and current_time - last_flush_time > 5):
                        self.flush_batch()
                        last_flush_time = current_time
                    
                    live.update(self.create_stats_table())
                    
        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down...[/yellow]")
        finally:
            self.flush_batch()
            if self.consumer:
                self.consumer.close()
            if self.clickhouse_client:
                self.clickhouse_client.close()
            console.print(f"[green]Total: {self.orders_inserted:,} orders, ฿{self.total_revenue:,.2f}[/green]")
    
    def stop(self):
        self.running = False


def main():
    parser = argparse.ArgumentParser(description='Sales Data Consumer')
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--bootstrap', type=str, default='localhost:9092')
    parser.add_argument('--clickhouse-host', type=str, default='localhost')
    parser.add_argument('--clickhouse-port', type=int, default=8123)
    args = parser.parse_args()
    
    consumer = SalesConsumer(
        args.bootstrap, args.clickhouse_host, args.clickhouse_port, args.batch_size
    )
    
    def signal_handler(sig, frame):
        consumer.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer.run()


if __name__ == '__main__':
    main()
