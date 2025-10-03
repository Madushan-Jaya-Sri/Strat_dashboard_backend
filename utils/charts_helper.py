# In your meta_manager.py or create a new charts_helper.py

from typing import Dict, List, Any
from collections import defaultdict

class ChartsDataTransformer:
    """Transform insights data into chart-ready formats"""
    
    @staticmethod
    def prepare_pie_chart_data(data: Dict, metric_key: str, label_key: str) -> List[Dict]:
        """
        Transform data for pie charts (distribution/proportion views)
        
        Example: Distribution of ad spend across campaigns
        """
        return [
            {
                'label': item.get(label_key, 'Unknown'),
                'value': item.get(metric_key, 0)
            }
            for item in data
        ]
    
    @staticmethod
    def prepare_bar_chart_data(items: List[Dict], metric_key: str, label_key: str, sort_by_value: bool = True) -> Dict:
        """
        Transform data for bar charts (comparison across categories)
        
        Example: Compare total impressions across different ad sets
        """
        chart_data = [
            {
                'label': item.get(label_key, 'Unknown'),
                'value': item.get(metric_key, 0)
            }
            for item in items
        ]
        
        if sort_by_value:
            chart_data.sort(key=lambda x: x['value'], reverse=True)
        
        return {
            'labels': [d['label'] for d in chart_data],
            'values': [d['value'] for d in chart_data]
        }
    
    @staticmethod
    def prepare_line_chart_data(timeseries: List[Dict], metrics: List[str]) -> Dict:
        """
        Transform time-series data for line charts
        
        Example: Track impressions and reach over time
        """
        return {
            'dates': [day['date'] for day in timeseries],
            'datasets': {
                metric: [day.get(metric, 0) for day in timeseries]
                for metric in metrics
            }
        }
    
    @staticmethod
    def prepare_stacked_bar_chart_data(timeseries: List[Dict], metrics: List[str]) -> Dict:
        """
        Transform data for stacked bar charts
        
        Example: Show organic vs paid impressions over time
        """
        return {
            'dates': [day['date'] for day in timeseries],
            'datasets': [
                {
                    'label': metric,
                    'data': [day.get(metric, 0) for day in timeseries]
                }
                for metric in metrics
            ]
        }
    
    @staticmethod
    def prepare_funnel_chart_data(summary: Dict, funnel_metrics: List[tuple]) -> List[Dict]:
        """
        Transform data for funnel charts
        
        Example: impressions -> clicks -> conversions
        funnel_metrics = [('impressions', 'Impressions'), ('clicks', 'Clicks'), ('conversions', 'Conversions')]
        """
        return [
            {
                'stage': label,
                'value': summary.get(key, 0)
            }
            for key, label in funnel_metrics
        ]
    
    @staticmethod
    def prepare_heatmap_data(timeseries: List[Dict], metric: str) -> List[Dict]:
        """
        Transform data for heatmap (day of week patterns)
        """
        from datetime import datetime
        
        day_hour_data = defaultdict(lambda: defaultdict(int))
        
        for day in timeseries:
            date_obj = datetime.strptime(day['date'], '%Y-%m-%d')
            day_name = date_obj.strftime('%A')
            day_hour_data[day_name]['total'] += day.get(metric, 0)
            day_hour_data[day_name]['count'] += 1
        
        return [
            {
                'day': day,
                'average': data['total'] / data['count'] if data['count'] > 0 else 0
            }
            for day, data in day_hour_data.items()
        ]
    
    @staticmethod
    def calculate_engagement_metrics(items: List[Dict]) -> List[Dict]:
        """
        Calculate engagement rates for comparison charts
        """
        return [
            {
                'name': item.get('name', 'Unknown'),
                'engagement_rate': (
                    (item.get('clicks', 0) + item.get('conversions', 0)) / item.get('impressions', 1) * 100
                    if item.get('impressions', 0) > 0 else 0
                )
            }
            for item in items
        ]