import sys
from PIL import Image

def create_gaussian_array(startPoint, myImage, length = None):
    outVal = []
    print "Making array!"
    if length is not None:
        for y in range(startPoint[1], startPoint[1] + length, 1):
            myRow = []
            for x in range(startPoint[0], startPoint[0] + length, 1):
                r, g, b = myImage.getpixel((x, y))
                myRow.append([r * g * b])
                myRow[len(myRow) - 1].append((r, g, b))
            outVal.append(myRow)
    else:
        for y in range(startPoint[1], myImage.size[1], 1):
            myRow = []
            for x in range(startPoint[0], myImage.size[0], 1):
                r, g, b = myImage.getpixel((x, y))
                myRow.append([r * g * b])
                myRow[len(myRow) - 1].append((r, g, b))
            outVal.append(myRow)
    print "Finished making array!"
    return outVal

def get_blue_color_no_green(colorPic, bwPic, elevation):
    outVal = None
    blueGauss = create_gaussian_array((0, 0), colorPic)
    for y in range(0, len(blueGauss), 1):
        for x in range(0, len(blueGauss[y]), 1):
            myPoint = blueGauss[y][x][1]
            if myPoint[0] <= 160:
                mySquare = colorPic.crop((x, y, x + 1, y + 1))
                bwPic.paste(mySquare, (x, y + elevation, x + 1, y + elevation + 1))
    outVal = bwPic
    return outVal


def get_blue_color_with_green(colorPic, bwPic, elevation):
    outVal = None
    blueGauss = create_gaussian_array((0, 0), colorPic)
    for y in range(0, len(blueGauss), 1):
        for x in range(0, len(blueGauss[y]), 1):
            myPoint = blueGauss[y][x][1]
            if myPoint[0] <= 160:
                if myPoint[2] > myPoint[0]:
                    mySquare = colorPic.crop((x, y, x + 1, y + 1))
                    bwPic.paste(mySquare, (x, y + elevation, x + 1, y + elevation + 1))
    outVal = bwPic
    return outVal

def get_vertical_image_strip(image, start, length):
    outVal = None
    ysize = image.size[1]
    outVal = image.crop((start, 0, start + length, ysize))
    return outVal

def get_rolled_image(image, delta):
    xsize, ysize = image.size
    delta = delta % xsize

    if delta == 0:
        return image
    part1 = image.crop((0, 0, delta, ysize))
    part2 = image.crop((delta, 0, xsize, ysize))
    image.paste(part2, (0, 0, xsize - delta, ysize))
    image.paste(part1, (xsize - delta, 0, xsize, ysize))
    return image

def print_gauss_array(myArray, fileName):
    myString = ""
    print "Starting to write file!"
    myFizzle = open(fileName+".txt", "w")
    print "Reading image with " + str(len(myArray)) + "."
    for y in range(0, len(myArray), 1):
        myString = ""
        for x in range(0, len(myArray[y]), 1):
            row = myArray[y][x]
            try:
                myString += str(row[0]) + "-(" + str(row[1][0]) + ", " + str(row[1][1]) + ", " + str(row[1][2]) + ")-" + str(round(float(float(float(row[1][0])/float(row[1][1]))/float(row[1][2])), 4)) + "|"
            except ZeroDivisionError:
                myString += str(row[0]) + "-(" + str(row[1][0]) + ", " + str(row[1][1]) + ", " + str(row[1][2]) + ")-NA|"
        myFizzle.write(myString + "\n")
    myFizzle.close()
    print "Finished writing to file!"

def main():
    try:
        myImage = Image.open(sys.argv[1])
        imageStrip = get_vertical_image_strip(myImage, 474, 122)
        leftOfStrip = myImage.crop((0, 0, 474, myImage.size[1]))
        rightOfStrip = myImage.crop((474 + 122, 0, myImage.size[0], myImage.size[1]))
        leftOfStrip.save("left_part.jpg", "JPEG")
        rightOfStrip.save("right_part.jpg", "JPEG")
        myGaussianSurface = create_gaussian_array((0, 0), imageStrip)
        bwStrip = imageStrip.convert("LA").convert("RGB")
        bwLeftOfStrip = leftOfStrip.convert("LA").convert("RGB")
        bwRightOfStrip = rightOfStrip.convert("LA").convert("RGB")
        rightGaussSurface = create_gaussian_array((0, 0), rightOfStrip)
        leftGaussSurface = create_gaussian_array((0, 0), leftOfStrip)
        #pixelCount = 0
        ###########################################################################################################################
        for y in range(0, len(myGaussianSurface), 1):
            for x in range(0, len(myGaussianSurface[y]), 1):
                pixelItem = myGaussianSurface[y][x]
                if pixelItem[0] <= 6700000 and pixelItem[0] >= 45000 and ((pixelItem[1][0] - pixelItem[1][2]) > 90):
                    mySquare = imageStrip.crop((x, y, x + 1, y + 1))
                    bwStrip.paste(mySquare, (x, y, x + 1, y + 1))
                elif pixelItem[0] == 0 and ((pixelItem[1][0] - pixelItem[1][2]) > 90):
                    mySquare = imageStrip.crop((x, y, x + 1, y + 1))
                    bwStrip.paste(mySquare, (x, y, x + 1, y + 1))
        ############################################################################################################################
        ##trying to make left of strip all yellow on bw greyscale image
        for y in range(0, len(leftGaussSurface), 1):
            for x in range(0, len(leftGaussSurface[y]), 1):
                pixelItem = leftGaussSurface[y][x]
                if pixelItem[0] <= 6700000 and pixelItem[0] >= 45000 and ((pixelItem[1][0] - pixelItem[1][2]) > 90):
                    mySquare = leftOfStrip.crop((x, y, x + 1, y + 1))
                    bwLeftOfStrip.paste(mySquare, (x, y, x + 1, y + 1))
                elif pixelItem[0] == 0 and ((pixelItem[1][0] - pixelItem[1][2]) > 90):
                    mySquare = leftOfStrip.crop((x, y, x + 1, y + 1))
                    bwLeftOfStrip.paste(mySquare, (x, y, x + 1, y + 1))
        bwLeftOfStrip.save("bw_left_of_strip.jpg", "JPEG")
        #############################################################################################################################
        ##trying to make right of strip all yellow on bw greyscale image
        for y in range(0, len(rightGaussSurface), 1):
            for x in range(0, len(rightGaussSurface[y]), 1):
                pixelItem = rightGaussSurface[y][x]
                if pixelItem[0] <= 6700000 and pixelItem[0] >= 45000 and ((pixelItem[1][0] - pixelItem[1][2]) > 90):
                    mySquare = rightOfStrip.crop((x, y, x + 1, y + 1))
                    bwRightOfStrip.paste(mySquare, (x, y, x + 1, y + 1))
                elif pixelItem[0] == 0 and ((pixelItem[1][0] - pixelItem[1][2]) > 90):
                    mySquare = rightOfStrip.crop((x, y, x + 1, y + 1))
                    bwRightOfStrip.paste(mySquare, (x, y, x + 1, y + 1))
        bwLeftOfStrip.save("bw_right_of_strip.jpg", "JPEG")
        




        blueBlock = imageStrip.crop((0, 340, 122, 440))
        blueBirdBlock = imageStrip.crop((0, 725, 122, 880))
        lastBlueBlock = imageStrip.crop((0, 1100, 122, 1200))
        firstBlock = imageStrip.crop((0, 130, 122, 190))
        secondBlock = imageStrip.crop((0, 220, 122, 290))

        leftBlueBlock = leftOfStrip.crop((0, 340, leftOfStrip.size[0], 440))
        leftBlueBirdBlock = leftOfStrip.crop((0, 725, leftOfStrip.size[0], 880))
        leftLastBlueBlock = leftOfStrip.crop((0, 1100, leftOfStrip.size[0], 1250))
        leftFirstBlock = leftOfStrip.crop((0, 130, leftOfStrip.size[0], 190))
        leftSecondBlock = leftOfStrip.crop((0, 220, leftOfStrip.size[0], 290))

        rightBlueBlock = rightOfStrip.crop((0, 340, rightOfStrip.size[0], 440))
        rightBlueBirdBlock = rightOfStrip.crop((0, 725, rightOfStrip.size[0], 880))
        rightLastBlueBlock = rightOfStrip.crop((0, 1100, rightOfStrip.size[0], 1250))
        rightFirstBlock = rightOfStrip.crop((0, 130, rightOfStrip.size[0], 190))
        rightSecondBlock = rightOfStrip.crop((0, 220, rightOfStrip.size[0], 290))

        bwStrip = get_blue_color_no_green(blueBirdBlock, get_blue_color_no_green(blueBlock, bwStrip, 340), 725)
        bwStrip = get_blue_color_no_green(firstBlock, get_blue_color_with_green(lastBlueBlock, bwStrip, 1100), 130)
        bwStrip = get_blue_color_no_green(secondBlock, bwStrip, 220)

        bwLeftOfStrip = get_blue_color_no_green(leftBlueBirdBlock, get_blue_color_no_green(leftBlueBlock, bwLeftOfStrip, 340), 725)
        bwLeftOfStrip = get_blue_color_no_green(leftFirstBlock, get_blue_color_with_green(leftLastBlueBlock, bwLeftOfStrip, 1100), 130)
        bwLeftOfStrip = get_blue_color_no_green(leftSecondBlock, bwLeftOfStrip, 220)

        bwRightOfStrip = get_blue_color_no_green(rightBlueBirdBlock, get_blue_color_no_green(rightBlueBlock, bwRightOfStrip, 340), 725)
        bwRightOfStrip = get_blue_color_no_green(rightFirstBlock, get_blue_color_with_green(rightLastBlueBlock, bwRightOfStrip, 1100), 130)
        bwRightOfStrip = get_blue_color_no_green(rightSecondBlock, bwRightOfStrip, 220)

        compositeImage = Image.new("RGB", (myImage.size[0] + (bwStrip.size[0] * 9), myImage.size[1]), (255, 255, 255))
        compositeImage.paste(bwLeftOfStrip, (0, 0, bwLeftOfStrip.size[0], bwLeftOfStrip.size[1]))
        for i in range(0, 10, 1):
            compositeImage.paste(bwStrip, (bwLeftOfStrip.size[0] + (bwStrip.size[0] * i), 0, bwLeftOfStrip.size[0] + (bwStrip.size[0] * (i + 1)), bwStrip.size[1]))
        compositeImage.paste(bwRightOfStrip, (bwLeftOfStrip.size[0] + (10 * bwStrip.size[0]), 0, bwLeftOfStrip.size[0] + (10 * bwStrip.size[0]) + bwRightOfStrip.size[0], bwRightOfStrip.size[1]))
        

        
        bwStrip.save("yellow_bw.jpg", "JPEG")
        imageStrip.save("image_strip.jpg", "JPEG")
        compositeImage.save("composite_image.jpg", "JPEG")
    except IndexError:
        print "Usage: clean_image.py my_image.jpg"

if __name__ == "__main__":
    main()

