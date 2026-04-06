cat > /mnt/hdfs/osm_to_csv.py << 'EOF'
import osmium, csv, os
from shapely.wkt import dumps
from shapely.geometry import Point, LineString, Polygon

OUTPUT_DIR = "/mnt/hdfs/osm_csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

class OSMHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.bf = open(f"{OUTPUT_DIR}/brazil_buildings.csv", "w", newline="")
        self.hf = open(f"{OUTPUT_DIR}/brazil_highways.csv", "w", newline="")
        self.pf = open(f"{OUTPUT_DIR}/brazil_points.csv", "w", newline="")

        self.bw = csv.writer(self.bf, quoting=csv.QUOTE_ALL)
        self.hw = csv.writer(self.hf, quoting=csv.QUOTE_ALL)
        self.pw = csv.writer(self.pf, quoting=csv.QUOTE_ALL)

        for w in [self.bw, self.hw, self.pw]:
            w.writerow(["geo", "desc"])

        self.cb = self.ch = self.cp = 0

    def node(self, n):
        if not n.tags:
            return

        desc = (
            n.tags.get("amenity")
            or n.tags.get("shop")
            or n.tags.get("tourism")
            or n.tags.get("natural")
            or "place"
        )

        try:
            self.pw.writerow([
                dumps(Point(n.location.lon, n.location.lat)),
                desc
            ])
            self.cp += 1

            if self.cp % 100000 == 0:
                print(f"Puntos: {self.cp:,}", flush=True)
        except:
            pass

    def way(self, w):
        if not w.tags or not w.nodes:
            return

        try:
            coords = [
                (n.lon, n.lat)
                for n in w.nodes
                if n.location.valid()
            ]

            if len(coords) < 2:
                return

            if w.tags.get("building") or w.tags.get("amenity"):
                if len(coords) >= 4 and coords[0] == coords[-1]:
                    desc = (
                        w.tags.get("amenity")
                        or w.tags.get("building")
                        or "building"
                    )

                    g = Polygon(coords)
                    if g.is_valid:
                        self.bw.writerow([dumps(g), desc])
                        self.cb += 1

                        if self.cb % 100000 == 0:
                            print(f"Edificios: {self.cb:,}", flush=True)

            elif w.tags.get("highway"):
                g = LineString(coords)
                if g.is_valid:
                    self.hw.writerow([
                        dumps(g),
                        w.tags.get("highway", "road")
                    ])
                    self.ch += 1

                    if self.ch % 100000 == 0:
                        print(f"Carreteras: {self.ch:,}", flush=True)

        except:
            pass

    def close(self):
        self.bf.close()
        self.hf.close()
        self.pf.close()

        print(f"\nEdificios: {self.cb:,}")
        print(f"Carreteras: {self.ch:,}")
        print(f"Puntos: {self.cp:,}")

handler = OSMHandler()
handler.apply_file("/mnt/hdfs/brazil-latest.osm.pbf", locations=True, idx="flex_mem")
handler.close()
EOF