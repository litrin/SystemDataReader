import optparse
import sys

from DataReader.Emon import EDPFormulas


def get_args():
    parser = optparse.OptionParser()
    parser.add_option("-i", "--input", dest="input",
                      default="metrics.xml",
                      help="metrics xml file", metavar="FILE")

    parser.add_option("-o", "--output", dest="output",
                      default="None",
                      help="output file name", metavar="FILE")

    options, args = parser.parse_args()

    return options


def main(input_file, output_file):
    formula_list = EDPFormulas(input_file)
    if output_file is None:
        fd = sys.stdout
    else:
        fd = open(output_file, "w")
    for f in formula_list:
        fd.writelines(str(f))
        fd.writelines("\n")
    fd.close()


if __name__ == "__main__":
    params = get_args()
    main(params.input, params.output)
