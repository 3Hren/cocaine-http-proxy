Le plan:

- [ ] Chose HTTP framework (mini-http or hyper).
- [ ] Possibly squash threads with event loops.
- [ ] Add monitoring port.
- [ ] Add primitive metrics.
- [ ] Implement dynamic service creation.
- [ ] Periodic reconnection.
- [ ] Rates per application.
- [ ] Histograms for response times per application.

```json
{
    "connections": {
        "accepted": 100,
        "progress": 32,
    },
    "requests": {
        "xxx": 100000,
        "2xx": 90000,
        "3xx": 0,
        "4xx": 2000,
        "5xx": 8000,
        "rate": [232.12, 230.21, 212.22],
    },
    "services": {
        "echo": {
            "pool": 10,
            ""
        }
    }
}
```
