def main():
    parser = argparse.ArgumentParser(description='.')
    parser.add_argument("-d", '--data',
                        help="Data set to use: small or big",
                        required=True)
    args = parser.parse_args()
    print(args)

    if args.data == "small":
        small_data_prediction()
    else:
        big_data_prediction()


if __name__ == "__main__":
    main()
