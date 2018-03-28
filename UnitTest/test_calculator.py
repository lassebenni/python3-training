import unittest
import calculator


class TestCalc(unittest.TestCase):
    # formula for unittest method name is test_functionName_testDescription
    def test_add(self):
        self.assertEqual(calculator.add(10, 5), 15)
        self.assertEqual(calculator.add(-1, 1), 0)
        self.assertEqual(calculator.add(-1, -2), -3)

    def test_subtract(self):
        self.assertEqual(calculator.subtract(10, 5), 5)
        self.assertEqual(calculator.subtract(-1, 1), -2)
        self.assertEqual(calculator.subtract(-1, -1), 0)

    def test_multiplication(self):
        self.assertEqual(calculator.multiply(10, 5), 50)
        self.assertEqual(calculator.multiply(-1, 1), -1)
        self.assertEqual(calculator.multiply(-1, -1), 1)
        self.assertEqual(calculator.multiply(0, 2), 0)
        self.assertEqual(calculator.multiply(2, 0), 0)

    def test_divide(self):
        self.assertEqual(calculator.divide(10, 5), 2)
        self.assertEqual(calculator.divide(-1, 1), -1)
        self.assertEqual(calculator.divide(-1, -1), 1)
        self.assertEqual(calculator.divide(0, 1), 0)
        self.assertEqual(calculator.divide(5, 2), 2.5)

        # self.assertRaises(ValueError, calculator.divide, 10, 0)
        with self.assertRaises(ValueError):
            calculator.divide(10, 0)


# Run the test
if __name__ == '__main__':
    unittest.main()