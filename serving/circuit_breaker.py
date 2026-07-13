import pybreaker
import logging

logger = logging.getLogger("serving.circuit_breaker")

class CircuitBreakerListener(pybreaker.CircuitBreakerListener):
    def state_change(self, cb, old_state, new_state):
        logger.warning(f"Circuit breaker state changed from {old_state.name} to {new_state.name}")

# Opens the circuit after 3 failures, tries to close after 10 seconds.
redis_breaker = pybreaker.CircuitBreaker(
    fail_max=3, 
    reset_timeout=10, 
    listeners=[CircuitBreakerListener()]
)
