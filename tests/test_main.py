from wind_turbines.main import main


def test_main() -> None:
    # Given some input data

    # When we call the function
    actual = main()

    # Then the result is as expected
    expected = 1
    assert actual == expected
