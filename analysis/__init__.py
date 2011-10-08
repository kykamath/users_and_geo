from library.classes import GeneralMethods
from library.geo import geographicConvexHull
from library.geo import getLocationFromLid

class SpotsKML:
    def __init__(self):
        import simplekml
        self.kml = simplekml.Kml()
    def addLidsWithHull(self, points, color=None, withAllPoints=False): self.addLocationPointsWithHull([getLocationFromLid(point) for point in points], color, withAllPoints)
    def addLocationPointsWithHull(self, points, color=None, withAllPoints=False):
        if not color: color=GeneralMethods.getRandomColor()
        points = [list(reversed(point)) for point in points]
        if not withAllPoints: self.kml.newpoint(coords=[points[0]])
        else: [self.kml.newpoint(coords=[point]) for point in points]
        pol=self.kml.newpolygon(outerboundaryis=geographicConvexHull(points))
        pol.polystyle.color = '99'+color[1:]
        pol.polystyle.outline = 0
    def addLocationPoints(self, points, color=None): 
        if not color: color=GeneralMethods.getRandomColor()
        for point in (list(reversed(point)) for point in points):
            pnt = self.kml.newpoint(coords=[point])
            pnt.iconstyle.icon.href = 'http://maps.google.com/mapfiles/kml/shapes/shaded_dot.png'
            pnt.iconstyle.color = 'ff'+color[1:]
    def addLine(self, points, description=None):
        from simplekml import LineStyle
        points = [list(reversed(getLocationFromLid(point))) for point in points]
        ls = self.kml.newlinestring(coords=points)
        self.kml.newpoint(coords=[points[0]]), self.kml.newpoint(coords=[points[-1]])
        if description: ls.description = description
        ls.linestyle = LineStyle(width=3.0)
    def write(self, fileName): self.kml.save(fileName)
    @staticmethod
    def drawKMLsForSpots(locationsIterator, outputKMLFile):
        kml = SpotsKML()
        for locations in locationsIterator: kml.addLidsWithHull(locations)
        kml.write(outputKMLFile)
    @staticmethod
    def drawKMLsForSpotsWithPoints(locationsIterator, outputKMLFile):
        kml = SpotsKML()
        for l in  list(locationsIterator): kml.addLocationPoints(l)
        kml.write(outputKMLFile)
    @staticmethod
    def drawKMLsForPoints(pointsIterator, outputKMLFile, color=None):
        kml = SpotsKML()
        if not color: color = GeneralMethods.getRandomColor()
        kml.addLocationPoints(pointsIterator, color=color)
        kml.write(outputKMLFile)