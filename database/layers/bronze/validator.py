"""
Bronze Layer: Data Validators
Location: database/layers/bronze/validators.py

Validates raw data before loading
"""

import re
from typing import Tuple, Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class BronzeValidator:
    """Validate data before loading to bronze"""
    
    @staticmethod
    def validate_products(data: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        if not data or not isinstance(data, list):
            return False, "Data must be non-empty list"
        required = ['id', 'title', 'price']
        for rec in data:
            for field in required:
                if field not in rec or rec[field] is None:
                    return False, f"Missing {field}"
            try:
                if float(rec.get('price', 0)) < 0:
                    return False, f"Negative price"
            except ValueError:
                return False, f"Invalid price"
        return True, None
    
    @staticmethod
    def validate_orders(data: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        if not data or not isinstance(data, list):
            return False, "Data must be non-empty list"
        for rec in data:
            if not rec.get('id') or not rec.get('userId'):
                return False, "Missing id or userId"
        return True, None
    
    @staticmethod
    def validate_users(data: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        if not data or not isinstance(data, list):
            return False, "Data must be non-empty list"
        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        for rec in data:
            if not rec.get('id'):
                return False, "Missing id"
            if 'email' in rec and rec['email']:
                if not re.match(email_regex, rec['email']):
                    return False, f"Invalid email"
        return True, None
    
    @staticmethod
    def validate_carts(data: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        if not data or not isinstance(data, list):
            return False, "Data must be non-empty list"
        for rec in data:
            if not rec.get('id') or not rec.get('userId'):
                return False, "Missing id or userId"
        return True, None
