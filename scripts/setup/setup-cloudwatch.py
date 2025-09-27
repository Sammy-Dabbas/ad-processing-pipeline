#!/usr/bin/env python3
"""
AWS CloudWatch Integration Setup
Connect your monitoring to real AWS CloudWatch
"""
import boto3
import json
import time
from datetime import datetime

class CloudWatchSetup:
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
        self.logs = boto3.client('logs', region_name='us-east-1')

    def setup_cloudwatch_integration(self):
        """Set up CloudWatch monitoring for ad event system"""

        print("  Setting up AWS CloudWatch Integration...")

        # 1. Create CloudWatch Log Groups
        log_groups = [
            '/aws/adevent/api',
            '/aws/adevent/consumer',
            '/aws/adevent/generator',
            '/aws/adevent/errors'
        ]

        for log_group in log_groups:
            try:
                self.logs.create_log_group(logGroupName=log_group)
                print(f" Created log group: {log_group}")
            except self.logs.exceptions.ResourceAlreadyExistsException:
                print(f" Log group exists: {log_group}")

        # 2. Create Custom Metrics Namespace
        namespace = "AdEventProcessing"

        # Send test metrics
        test_metrics = [
            {
                'MetricName': 'EventsPerSecond',
                'Value': 1000,
                'Unit': 'Count/Second',
                'Dimensions': [{'Name': 'Service', 'Value': 'EventProcessor'}]
            },
            {
                'MetricName': 'ProcessingLatency',
                'Value': 25.5,
                'Unit': 'Milliseconds',
                'Dimensions': [{'Name': 'Service', 'Value': 'EventProcessor'}]
            }
        ]

        try:
            self.cloudwatch.put_metric_data(
                Namespace=namespace,
                MetricData=test_metrics
            )
            print(" Custom metrics namespace created")
        except Exception as e:
            print(f" Error creating metrics: {e}")

        # 3. Create CloudWatch Alarms
        alarms = [
            {
                'AlarmName': 'AdEvent-HighLatency',
                'ComparisonOperator': 'GreaterThanThreshold',
                'EvaluationPeriods': 2,
                'MetricName': 'ProcessingLatency',
                'Namespace': namespace,
                'Period': 300,
                'Statistic': 'Average',
                'Threshold': 50.0,
                'ActionsEnabled': True,
                'AlarmActions': [],
                'AlarmDescription': 'Processing latency above 50ms',
                'Unit': 'Milliseconds'
            },
            {
                'AlarmName': 'AdEvent-LowThroughput',
                'ComparisonOperator': 'LessThanThreshold',
                'EvaluationPeriods': 3,
                'MetricName': 'EventsPerSecond',
                'Namespace': namespace,
                'Period': 300,
                'Statistic': 'Average',
                'Threshold': 10000.0,
                'ActionsEnabled': True,
                'AlarmActions': [],
                'AlarmDescription': 'Event processing below 10K/sec',
                'Unit': 'Count/Second'
            }
        ]

        for alarm in alarms:
            try:
                self.cloudwatch.put_metric_alarm(**alarm)
                print(f" Created alarm: {alarm['AlarmName']}")
            except Exception as e:
                print(f" Error creating alarm {alarm['AlarmName']}: {e}")

        # 4. Create Dashboard
        dashboard_body = {
            "widgets": [
                {
                    "type": "metric",
                    "properties": {
                        "metrics": [
                            [namespace, "EventsPerSecond", "Service", "EventProcessor"],
                            [namespace, "ProcessingLatency", "Service", "EventProcessor"]
                        ],
                        "period": 300,
                        "stat": "Average",
                        "region": "us-east-1",
                        "title": "Ad Event Processing Performance"
                    }
                },
                {
                    "type": "log",
                    "properties": {
                        "query": "SOURCE '/aws/adevent/errors'\n| fields @timestamp, @message\n| sort @timestamp desc\n| limit 100",
                        "region": "us-east-1",
                        "title": "Recent Errors"
                    }
                }
            ]
        }

        try:
            self.cloudwatch.put_dashboard(
                DashboardName='AdEventProcessing',
                DashboardBody=json.dumps(dashboard_body)
            )
            print(" Created CloudWatch dashboard")
        except Exception as e:
            print(f" Error creating dashboard: {e}")

        print("\n CloudWatch setup complete!")
        print(" Dashboard: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=AdEventProcessing")

    def send_test_metrics(self):
        """Send test metrics to verify setup"""
        print(" Sending test metrics...")

        metrics = []
        for i in range(10):
            metrics.extend([
                {
                    'MetricName': 'EventsPerSecond',
                    'Value': 15000 + (i * 1000),
                    'Unit': 'Count/Second',
                    'Timestamp': datetime.utcnow(),
                    'Dimensions': [{'Name': 'Service', 'Value': 'EventProcessor'}]
                },
                {
                    'MetricName': 'ProcessingLatency',
                    'Value': 20 + (i * 2),
                    'Unit': 'Milliseconds',
                    'Timestamp': datetime.utcnow(),
                    'Dimensions': [{'Name': 'Service', 'Value': 'EventProcessor'}]
                }
            ])

        try:
            self.cloudwatch.put_metric_data(
                Namespace='AdEventProcessing',
                MetricData=metrics
            )
            print(" Test metrics sent successfully")
        except Exception as e:
            print(f" Error sending metrics: {e}")

if __name__ == "__main__":
    setup = CloudWatchSetup()
    setup.setup_cloudwatch_integration()
    setup.send_test_metrics()