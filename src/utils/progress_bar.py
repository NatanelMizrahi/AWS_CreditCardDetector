import os
from config.app_config import PROGRESS_BAR_CLEAR_SCREEN


# Print iterations progress
def printProgressBar (iteration, total,
                      prefix='\033[2JProgress:', suffix='Complete', length=50, decimals = 2, fill = '█', printEnd = "\r\n"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    if PROGRESS_BAR_CLEAR_SCREEN:
        os.system('cls||clear')
    # Print New Line on Complete
    if iteration == total:
        print()