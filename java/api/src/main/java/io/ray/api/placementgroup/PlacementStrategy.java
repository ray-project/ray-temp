package io.ray.api.placementgroup;

public enum PlacementStrategy {
  /**
   * Packs Bundles close together inside processes or nodes as tight as possible.
   */
  PACK(0),
  /**
   * Places Bundles across distinct nodes or processes as even as possible.
   */
  SPREAD(1);

  private int value = 0;


  PlacementStrategy(int value) {
    this.value = value;
  }

  public int value() {
    return this.value;
  }
}
