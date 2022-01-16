for dir in ./tmp/*
do
    dir=${dir%*/}      # remove the trailing "/"
    echo "${dir##*/}"    # print everything after the final "/"
    file="./tmp/${dir##*/}/*.csv"
    cp $file "./${dir##*/}.csv"
    echo $file
    rm -r "./tmp/${dir##*/}"
done