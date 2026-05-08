# Known Issues

The validator's schema registration was refactored to be instance-scoped
(see `src/pipeline/validator.js`); some downstream tests in `test_router.js`
still depend on shared module-level state and need parallel updates.

To be addressed:
- Router test expectations for default route count
- A few middleware tests that depend on the old singleton validator pattern

Tracking in follow-up commits.
