"""
Kafka Topics Setup Script for Sales Analytics.

Creates required Kafka topics for the Sales Analytics Platform.

Usage:
    uv run python scripts/setup_kafka_topics.py
"""

import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from rich.console import Console
from rich.table import Table

console = Console()


def create_topics(bootstrap_servers: str):
    """Create required Kafka topics."""
    
    topics_config = [
        {
            'name': 'sales_orders',
            'partitions': 3,
            'replication_factor': 1,
            'description': 'Sales order events'
        },
    ]
    
    console.print(f"\n[bold]Connecting to Kafka at {bootstrap_servers}...[/bold]")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='sales_analytics_admin'
        )
    except NoBrokersAvailable:
        console.print("[red]Error: No Kafka brokers available. Make sure Kafka is running.[/red]")
        console.print("[yellow]Hint: Run 'docker compose up -d' in infra directory[/yellow]")
        return False
    
    results = []
    for topic in topics_config:
        new_topic = NewTopic(
            name=topic['name'],
            num_partitions=topic['partitions'],
            replication_factor=topic['replication_factor']
        )
        try:
            admin_client.create_topics([new_topic], validate_only=False)
            results.append((topic['name'], 'Created', 'green'))
            console.print(f"[green]Created topic: {topic['name']}[/green]")
        except TopicAlreadyExistsError:
            results.append((topic['name'], 'Already Exists', 'yellow'))
            console.print(f"[yellow]Topic already exists: {topic['name']}[/yellow]")
        except Exception as e:
            results.append((topic['name'], f'Error: {e}', 'red'))
            console.print(f"[red]Failed to create topic {topic['name']}: {e}[/red]")
    
    # Display summary
    console.print("\n")
    table = Table(title="Kafka Topics Summary")
    table.add_column("Topic", style="cyan")
    table.add_column("Partitions", justify="center")
    table.add_column("Status", justify="center")
    table.add_column("Description")
    
    for topic, result in zip(topics_config, results):
        status_color = result[2]
        table.add_row(
            topic['name'],
            str(topic['partitions']),
            f"[{status_color}]{result[1]}[/{status_color}]",
            topic['description']
        )
    
    console.print(table)
    admin_client.close()
    return True


def list_topics(bootstrap_servers: str):
    """List all existing Kafka topics."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='sales_analytics_admin'
        )
        topics = admin_client.list_topics()
        console.print("\n[bold]Existing Kafka Topics:[/bold]")
        for topic in sorted(topics):
            if not topic.startswith('_'):
                console.print(f"  - {topic}")
        admin_client.close()
    except NoBrokersAvailable:
        console.print("[red]Error: No Kafka brokers available.[/red]")


def main():
    parser = argparse.ArgumentParser(description='Setup Kafka topics for Sales Analytics')
    parser.add_argument('--bootstrap', type=str, default='localhost:9092')
    parser.add_argument('--list', action='store_true', help='List existing topics')
    args = parser.parse_args()
    
    if args.list:
        list_topics(args.bootstrap)
    else:
        create_topics(args.bootstrap)


if __name__ == '__main__':
    main()
