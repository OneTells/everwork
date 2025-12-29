def timer(*, weeks: int = 0, days: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0, milliseconds: int = 0) -> float:
    return weeks * 604800 + days * 86400 + hours * 3600 + minutes * 60 + seconds + milliseconds / 1000
