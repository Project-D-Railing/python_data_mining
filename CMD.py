import basecommand
import main

mCommands = {}


def printHelp():
    print("List of commands:")
    print("  help - print this message")
    print("  quit/exit - exit command line")
    for name, command in mCommands.items():
        command.printHelp()


def addCommands():
    mCommands["GetTrainHistory"] = main.GetTrainHistory()


addCommands()
while True:
        test = input("d-Railing>")
        if test == "quit" or test == "exit":
            print("Exiting")
            break
        if test == "help":
            printHelp()
            continue
        if test in mCommands:
            mCommands.get(test).execute(test)
            continue
        print("Unknown command: " + test)
        printHelp()
