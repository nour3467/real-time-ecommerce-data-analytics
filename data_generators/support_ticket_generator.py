from datetime import datetime
import uuid
import random
from typing import Dict, Any, Tuple
from .event_generator import BaseGenerator
from .config import SUPPORT_PATTERNS

class SupportTicketGenerator(BaseGenerator):
    def __init__(self, db_config: Dict[str, Any], kafka_config: Dict[str, Any]):
        super().__init__(db_config, kafka_config)
        self.load_active_data()
        self.fake = Faker()

    def load_active_data(self):
        """Load active users and orders"""
        self.active_users = self.execute_query("""
            SELECT user_id
            FROM users
            WHERE is_active = true
        """)

        self.recent_orders = self.execute_query("""
            SELECT order_id, user_id, status
            FROM orders
            WHERE created_at >= NOW() - INTERVAL '30 days'
        """)

        self.active_tickets = self.execute_query("""
            SELECT ticket_id, user_id, status
            FROM support_tickets
            WHERE status NOT IN ('resolved', 'closed')
        """)

    def generate_ticket(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Generate new support ticket with initial message"""
        if not self.recent_orders:
            return None, None

        order = random.choice(self.recent_orders)
        now = datetime.now()

        # Generate ticket
        ticket = {
            'ticket_id': str(uuid.uuid4()),
            'user_id': order['user_id'],
            'order_id': order['order_id'],
            'issue_type': random.choices(
                list(SUPPORT_PATTERNS['issue_types'].keys()),
                weights=list(SUPPORT_PATTERNS['issue_types'].values())
            )[0],
            'priority': random.choices(
                list(SUPPORT_PATTERNS['priority_distribution'].keys()),
                weights=list(SUPPORT_PATTERNS['priority_distribution'].values())
            )[0],
            'status': 'open',
            'created_at': now,
            'resolved_at': None,
            'satisfaction_score': None
        }

        # Generate initial message
        initial_message = {
            'message_id': str(uuid.uuid4()),
            'ticket_id': ticket['ticket_id'],
            'sender_type': 'customer',
            'message_text': self.generate_customer_message(ticket['issue_type'], order['status']),
            'created_at': now
        }

        return ticket, initial_message

    def generate_customer_message(self, issue_type: str, order_status: str) -> str:
        """Generate contextual customer message"""
        messages = {
            'order_status': [
                f"I haven't received any updates about my order. It's been {random.randint(2, 5)} days.",
                "Could you please check the status of my order?",
                "My order seems to be delayed. Can you help?"
            ],
            'delivery_delay': [
                "My package was supposed to arrive yesterday but hasn't shown up.",
                "The tracking hasn't updated in several days.",
                "I need to know if my delivery is still coming."
            ],
            'product_issue': [
                "The product I received doesn't match the description.",
                "Item arrived damaged. What should I do?",
                "Received wrong size/color. Need help with exchange."
            ],
            'payment_issue': [
                "I was charged twice for my order.",
                "The discount wasn't applied correctly.",
                "Need help with refund process."
            ],
            'return_refund': [
                "How do I return this item?",
                "Haven't received my refund yet.",
                "Need return shipping label."
            ]
        }
        return random.choice(messages.get(issue_type, ["Need assistance with my order."]))

    def generate_support_message(self, issue_type: str) -> str:
        """Generate support agent response"""
        templates = [
            "I understand your concern about {issue}. Let me help you with that.",
            "Thank you for reaching out about {issue}. I'll be happy to assist.",
            "I'm looking into your {issue} right now.",
            "I apologize for any inconvenience with {issue}. Let's resolve this."
        ]
        return random.choice(templates).format(issue=issue_type.replace('_', ' '))

    def update_ticket(self, ticket_data: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Update existing ticket and generate response"""
        now = datetime.now()
        current_status = ticket_data['status']

        # Determine next status
        status_flow = {
            'open': 'in_progress',
            'in_progress': 'resolved'
        }
        new_status = status_flow.get(current_status)

        if new_status:
            update_data = {
                'status': new_status,
                'resolved_at': now if new_status == 'resolved' else None
            }

            if new_status == 'resolved':
                # Add satisfaction score for resolved tickets
                update_data['satisfaction_score'] = random.choices(
                    list(SUPPORT_PATTERNS['satisfaction_score_distribution'].keys()),
                    weights=list(SUPPORT_PATTERNS['satisfaction_score_distribution'].values())
                )[0]

            self.update_record('support_tickets', ticket_data['ticket_id'], update_data, 'ticket_id')

            # Generate support message
            message = {
                'message_id': str(uuid.uuid4()),
                'ticket_id': ticket_data['ticket_id'],
                'sender_type': 'support_agent',
                'message_text': self.generate_support_message(ticket_data['issue_type']),
                'created_at': now
            }

            return new_status, message
        return current_status, None

    def generate_event(self) -> Dict[str, Any]:
        """Generate support ticket events"""
        # 70% chance to update existing ticket if available
        if self.active_tickets and random.random() < 0.7:
            ticket_data = random.choice(self.active_tickets)
            new_status, message = self.update_ticket(ticket_data)

            if message:
                self.insert_record('ticket_messages', message)
                return {
                    'type': 'ticket_update',
                    'ticket_id': ticket_data['ticket_id'],
                    'new_status': new_status,
                    'message': message
                }
        else:
            # Create new ticket
            ticket, message = self.generate_ticket()
            if ticket and message:
                self.insert_record('support_tickets', ticket)
                self.insert_record('ticket_messages', message)
                return {
                    'type': 'ticket_create',
                    'ticket': ticket,
                    'message': message
                }

    def run(self):
        """Run the support ticket generator"""
        while True:
            try:
                event = self.generate_event()
                if event:
                    self.send_event('support_tickets', event)

                # Support tickets generated every few minutes
                time.sleep(random.uniform(120, 300))

                # Refresh active data periodically
                if random.random() < 0.1:
                    self.load_active_data()

            except Exception as e:
                self.logger.error(f"Error in support ticket generator: {str(e)}")
                time.sleep(5)