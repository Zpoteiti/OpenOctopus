from openctopus_server.auth.password import hash_password, verify_password


def test_hash_and_verify_round_trip():
    hashed = hash_password("my-secret-password")
    assert hashed != "my-secret-password"
    assert verify_password("my-secret-password", hashed) is True


def test_verify_rejects_wrong_password():
    hashed = hash_password("correct-password")
    assert verify_password("wrong-password", hashed) is False
