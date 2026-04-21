"""
Data generation utilities for Kafka producer
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
from faker import Faker

fake = Faker()


class EventGenerator:
    """Generate realistic sample events."""
    
    EVENT_TYPES = ['login', 'logout', 'purchase', 'view_product', 'add_to_cart', 'checkout', 'signup', 'search', 'click']
    PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Food', 'Toys', 'Sports', 'Home', 'Beauty']
    
    @staticmethod
    def generate_user_event() -> Dict[str, Any]:
        """Generate a random user event."""
        event = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'event_id': fake.uuid4(),
            'event_type': fake.random_element(EventGenerator.EVENT_TYPES),
            'user_id': fake.uuid4(),
            'user_email': fake.email(),
            'session_id': fake.uuid4(),
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent(),
            'data': {
                'page_url': fake.url(),
                'referrer': fake.url() if fake.boolean() else None,
                'viewport_width': fake.random_int(768, 1920),
                'viewport_height': fake.random_int(600, 1080),
            }
        }
        return event
    
    @staticmethod
    def generate_purchase_event() -> Dict[str, Any]:
        """Generate a purchase event."""
        event = EventGenerator.generate_user_event()
        event['event_type'] = 'purchase'
        event['data'].update({
            'product_id': fake.uuid4(),
            'product_name': fake.word(),
            'category': fake.random_element(EventGenerator.PRODUCT_CATEGORIES),
            'quantity': fake.random_int(1, 5),
            'unit_price': round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
            'amount': round(fake.pyfloat(left_digits=4, right_digits=2, positive=True), 2),
            'tax': round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2),
            'currency': 'USD',
            'payment_method': fake.random_element(['credit_card', 'debit_card', 'paypal', 'apple_pay', 'google_pay']),
            'order_id': fake.uuid4(),
        })
        return event
    
    @staticmethod
    def generate_view_product_event() -> Dict[str, Any]:
        """Generate a view product event."""
        event = EventGenerator.generate_user_event()
        event['event_type'] = 'view_product'
        event['data'].update({
            'product_id': fake.uuid4(),
            'product_name': fake.word(),
            'category': fake.random_element(EventGenerator.PRODUCT_CATEGORIES),
            'price': round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
            'rating': round(fake.pyfloat(left_digits=1, right_digits=1, positive=True, min_value=1, max_value=5), 1),
            'in_stock': fake.boolean(),
            'time_on_page_seconds': fake.random_int(5, 300),
        })
        return event
    
    @staticmethod
    def generate_search_event() -> Dict[str, Any]:
        """Generate a search event."""
        event = EventGenerator.generate_user_event()
        event['event_type'] = 'search'
        event['data'].update({
            'search_query': fake.words(3),
            'results_count': fake.random_int(0, 1000),
            'selected_result_position': fake.random_int(1, 10) if fake.boolean() else None,
        })
        return event
    
    @staticmethod
    def generate_batch(size: int = 100) -> List[Dict[str, Any]]:
        """Generate a batch of random events."""
        events = []
        weights = [0.3, 0.15, 0.2, 0.15, 0.1, 0.05, 0.03, 0.02]  # Weights for each event type
        
        for _ in range(size):
            rand = fake.random.random()
            cumulative = 0
            
            if rand < cumulative + weights[0]:
                events.append(EventGenerator.generate_user_event())
            elif rand < cumulative + weights[1]:
                events.append(EventGenerator.generate_purchase_event())
            elif rand < cumulative + weights[2]:
                events.append(EventGenerator.generate_view_product_event())
            elif rand < cumulative + (weights[3] + weights[4] + weights[5] + weights[6] + weights[7]):
                events.append(EventGenerator.generate_user_event())
            else:
                events.append(EventGenerator.generate_search_event())
        
        return events
    
    @staticmethod
    def generate_timeseries_data(base_timestamp: datetime, num_events: int = 1000) -> List[Dict[str, Any]]:
        """Generate time-series data with timestamps distributed over time."""
        events = []
        current_timestamp = base_timestamp
        
        for _ in range(num_events):
            event = EventGenerator.generate_user_event()
            event['timestamp'] = current_timestamp.isoformat() + 'Z'
            events.append(event)
            # Add random seconds to timestamp for more realistic distribution
            current_timestamp += timedelta(seconds=fake.random_int(1, 60))
        
        return events
