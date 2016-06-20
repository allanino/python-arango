from __future__ import absolute_import, unicode_literals

import pytest

from arango.connection import Connection

conn = Connection()
wal = conn.wal


@pytest.mark.order1
def test_wal_options():
    options = wal.properties()
    assert 'oversized_ops' in options
    assert 'log_size' in options
    assert 'historic_logs' in options
    assert 'reserve_logs' in options


@pytest.mark.order2
def test_wal_set_options():
    wal.set_properties(
        historic_logs=15,
        oversized_ops=False,
        log_size=30000000,
        reserve_logs=5,
        throttle_limit=1000,
        throttle_wait=16000
    )
    options = conn.wal.properties()
    assert options['historic_logs'] == 15
    assert options['oversized_ops'] is False
    assert options['log_size'] == 30000000
    assert options['reserve_logs'] == 5
    assert options['throttle_limit'] == 1000
    assert options['throttle_wait'] == 16000


@pytest.mark.order3
def test_wal_transactions():
    result = wal.transactions()
    assert 'count' in result
    assert 'last_sealed' in result
    assert 'last_collected' in result


@pytest.mark.order4
def test_flush_wal():
    assert isinstance(wal.flush(), bool)
