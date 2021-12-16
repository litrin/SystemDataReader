import pandas as pd

from DataReader.ptumon import PtuReader


def conversation(input, output, devices):
    reader = PtuReader(input)
    devices = [i.upper() for i in devices]
    data = None

    for col in ("Power", "CFreq", "Volt", 'Util', "C0", "C1", "C6"):
        tmp = reader.get_telemetry(devices, col)
        tmp.columns = pd.MultiIndex.from_product([[col], devices])

        if data is None:
            data = tmp
        else:
            data = pd.concat([data, tmp], axis=1)
    print(output)
    data.to_excel(output)


def main():
    import optparse

    parser = optparse.OptionParser()

    parser.add_option("-i", "--input", dest="input",
                      default="ptumon.csv",
                      help="ptumon cvs file for input")

    parser.add_option("-o", "--output", default="ptumon.xlsx", dest="output",
                      help="Excel file name")

    parser.add_option("-c", "--devices", default="cpu0,cpu1",
                      dest="devices",
                      help="Component list, separated by ','")

    (options, args) = parser.parse_args()
    conversation(options.input, options.output, options.devices.split(","))


if __name__ == "__main__":
    main()
