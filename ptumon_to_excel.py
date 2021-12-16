import pandas as pd

from DataReader.ptumon import PtuReader

columns = ("Power", "CFreq", "Volt", 'Util', "C0", "C1", "C6")


def plot(input, output, devices):
    reader = PtuReader(input)
    reader.save_pdf(output, devices=devices, telemetries=columns)


def conversation(input, output, devices):
    reader = PtuReader(input)

    data = None

    for col in columns:
        tmp = reader.get_telemetry(devices, col)
        tmp.columns = pd.MultiIndex.from_product([[col], devices])

        if data is None:
            data = tmp
        else:
            data = pd.concat([data, tmp], axis=1)
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

    parser.add_option("-d", "--diagram", default=None, dest="diagram",
                      help="Draw diagrams with PDF format")

    (options, args) = parser.parse_args()
    devices = [i.upper() for i in options.devices.split(",")]
    if options.diagram is not None:
        plot(options.input, options.diagram, devices)
    else:
        conversation(options.input, options.output, devices)


if __name__ == "__main__":
    main()
