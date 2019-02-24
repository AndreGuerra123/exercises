import solution
import unittest

def test_all_files_summary():
    fr = solution.Fauxlizer('./exercises/file_0.faux')
    assert fr.get_summary()
    fr = solution.Fauxlizer('./exercises/file_1.faux')
    assert fr.get_summary()
    fr = solution.Fauxlizer('./exercises/file_3.faux')
    assert fr.get_summary()
    fr = solution.Fauxlizer('./exercises/file_4.faux')
    assert fr.get_summary()
    fr = solution.Fauxlizer('./exercises/file_5.faux')
    assert fr.get_summary()
    fr = solution.Fauxlizer('./exercises/file_7.faux')
    assert fr.get_summary()
    fr = solution.Fauxlizer('./exercises/file_9.faux')
    assert fr.get_summary()

class TestFailure(unittest.TestCase):
    def test_invalid_file(self):    
        with self.assertRaises(FileExistsError):
            solution.Fauxlizer('./exercises/file_6.faux')

