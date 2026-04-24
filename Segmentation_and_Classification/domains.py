"""
Application Domain Vocabulary

Fixed list of supported application domains.
This is a controlled vocabulary - no inference, no expansion.

Usage:
- One domain selected per project/run
- Domain is NOT inferred per chunk
- Domain provides high-level context for test generation
"""

APPLICATION_DOMAINS = [
    "E-commerce",
    "Finance",
    "Healthcare",
    "Education",
    "Manufacturing",
    "Government",
    "Gaming",
    "Social Media",
    "Restaurant/Food Service",
    "Location-based Services",
    "Task Management / Productivity Tools"
]