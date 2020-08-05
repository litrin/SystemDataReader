from DataReader.pqos import PQoSCSVReader, PQoSOutputReader, PQoSXMLReader
import optparse


def get_args():
    parser = optparse.OptionParser()
    parser.add_option("-i", "--input", dest="input",
                      default=None,
                      help="pqos file name", metavar="FILE")
    parser.add_option("-t", "--type", dest="input_type",
                      default=True,
                      help="input file type, supports csv, xml, raw. default: csv")

    parser.add_option("-o", "--output", dest="output",
                      default="pqos.xlsx",
                      help="output file name", metavar="FILE")
    parser.add_option("-p", "--plot", dest="plot",
                      default=False, action="store_false",
                      help="draw diagrams only")

    options, args = parser.parse_args()

    return options


def main(i, o, inputtype, plot=False):
    if inputtype == "csv":
        reader = PQoSCSVReader(i)
    elif inputtype == "xml":
        reader = PQoSXMLReader(i)
    else:
        reader = PQoSOutputReader(i)

    if plot:
        reader.plot(o)
    else:
        reader.to_excel(o, False)


if __name__ == "__main__":
    params = get_args()
    type = params.input_type.lower()
    main(params.input, params.output, type, params.plot)
