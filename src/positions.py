'''
To register the pair positions, and keep track of their continuous evs
'''

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from decimal import Decimal

@dataclass
class ArbitrageLeg:
    exchange:str
    ticker:str
    side: str
    size: Decimal 
    entry_price: Decimal
    order_id: Optional[str] = None
    fill_time: Optional[datetime] = None
    exit_price: Optional[Decimal] = None
    exit_time: Optional[datetime]= None
    

@dataclass
class ArbitragePosition:
    position_id: str
    asset: str
    legs: List[ArbitrageLeg]
    entry_time:datetime
    entry_funding_rate: float
    entry_basis: float
    target_profit: float
    stop_loss: float
    max_hold_time: timedelta = field(default_factory=lambda:timedelta(days=30))
    
    accumulated_funding:Decimal = Decimal('0')
    total_fees_paid: Decimal = Decimal('0')
    unrealized_pnl: float = 0.0
    
    # calculate the age of the position
    @property
    def age(self):
        return datetime.now() - self.entry_time
    
    # calculate the total earning (pnl) from the position
    @property
    def total_pnl(self):
        return self.accumulated_funding + self.unrealized_pnl - self.total_fees_paid
    
    # calculate the annualised return from the position
    @property
    def annualized_return(self):
        days_held = max(self.age.days, 1)
        total_invested = sum(leg.size *leg.entry_price for leg in self.legs)
        
        return float(self.total_pnl/ total_invested) * (365/days_held)
    
    