#!/usr/bin/env python3
"""
viewer_avatar.py - Abre um avatar 3D (.glb/.gltf) numa janela.

Instalacao:
    pip install panda3d panda3d-gltf

Uso:
    python viewer_avatar.py avatar.glb

Controles:
    arrastar com botao esquerdo : girar a camera
    arrastar com botao direito  : mover (pan)
    roda do mouse               : zoom
    1..9                        : tocar animacao N (se o modelo tiver)
    ESPACO                      : pausar / continuar
    R                           : resetar camera
    ESC                         : sair
"""
import math
import os
import sys

from direct.actor.Actor import Actor
from direct.gui.OnscreenText import OnscreenText
from direct.showbase.ShowBase import ShowBase
from panda3d.core import (AmbientLight, DirectionalLight, Filename, TextNode,
                          Vec3, loadPrcFileData)

loadPrcFileData("", "window-title Avatar Libras\nwin-size 1024 768\nmultisamples 4\naudio-library-name null")

try:
    import gltf  # panda3d-gltf
except ImportError:
    gltf = None


class Viewer(ShowBase):
    def __init__(self, path):
        super().__init__()
        if gltf is not None and hasattr(gltf, "patch_loader"):
            gltf.patch_loader(self.loader)

        self.setBackgroundColor(0.85, 0.88, 0.95)
        self.disableMouse()

        path = os.path.abspath(path)
        if not os.path.isfile(path):
            sys.exit(f"Arquivo nao encontrado: {path}")
        self.actor = Actor(Filename.from_os_specific(path))
        self.actor.reparentTo(self.render)
        self.actor.setTwoSided(True)

        self._setup_lights()
        self._frame_model()
        self._setup_animations()
        self._setup_controls()

        self.hud = OnscreenText(text="", pos=(-1.3, 0.92), scale=0.045,
                                align=TextNode.ALeft, fg=(0.1, 0.1, 0.15, 1), mayChange=True)
        self._update_hud()
        self.taskMgr.add(self._camera_task, "camera")

    # ---------------------------------------------------------------- cena
    def _setup_lights(self):
        amb = AmbientLight("amb")
        amb.setColor((0.55, 0.55, 0.6, 1))
        self.render.setLight(self.render.attachNewNode(amb))

        key = DirectionalLight("key")
        key.setColor((0.9, 0.9, 0.85, 1))
        knp = self.render.attachNewNode(key)
        knp.setHpr(-30, -35, 0)
        self.render.setLight(knp)

        fill = DirectionalLight("fill")
        fill.setColor((0.3, 0.3, 0.4, 1))
        fnp = self.render.attachNewNode(fill)
        fnp.setHpr(150, -20, 0)
        self.render.setLight(fnp)

    def _frame_model(self):
        mn, mx = self.actor.getTightBounds()
        self.target0 = (mn + mx) / 2
        self.radius = max((mx - mn).length() / 2, 0.01)
        self._reset_camera()

    def _reset_camera(self):
        self.target = Vec3(self.target0)
        self.heading, self.pitch = 0.0, 5.0
        self.dist = self.radius * 2.4

    # ----------------------------------------------------------- animacoes
    def _setup_animations(self):
        self.anims = list(self.actor.getAnimNames())
        self.current = None
        self.playing = False
        if self.anims:
            self._play(0)

    def _play(self, i):
        if i < len(self.anims):
            self.current = self.anims[i]
            self.actor.loop(self.current)
            self.playing = True
            self._update_hud()

    def _toggle(self):
        if not self.current:
            return
        if self.playing:
            self.actor.stop()
        else:
            self.actor.loop(self.current)
        self.playing = not self.playing
        self._update_hud()

    def _update_hud(self):
        if not hasattr(self, "hud"):
            return
        if self.anims:
            lines = [f"[{i + 1}] {n}{'  <' if n == self.current else ''}"
                     for i, n in enumerate(self.anims[:9])]
            txt = "\n".join(lines) + "\nESPACO: pausar"
        else:
            txt = "Modelo sem animacoes (pose estatica)"
        self.hud.setText(txt)

    # ------------------------------------------------------------ controles
    def _setup_controls(self):
        self.drag = None            # "rot" | "pan"
        self.last = None
        self.accept("mouse1", self._start_drag, ["rot"])
        self.accept("mouse1-up", self._stop_drag)
        self.accept("mouse3", self._start_drag, ["pan"])
        self.accept("mouse3-up", self._stop_drag)
        self.accept("wheel_up", self._zoom, [0.9])
        self.accept("wheel_down", self._zoom, [1.1])
        self.accept("space", self._toggle)
        self.accept("r", self._reset_camera)
        self.accept("escape", sys.exit)
        for n in range(1, 10):
            self.accept(str(n), self._play, [n - 1])

    def _start_drag(self, mode):
        self.drag = mode
        mw = self.mouseWatcherNode
        self.last = (mw.getMouseX(), mw.getMouseY()) if mw.hasMouse() else None

    def _stop_drag(self):
        self.drag = None

    def _zoom(self, f):
        self.dist = max(self.radius * 0.3, self.dist * f)

    def _camera_task(self, task):
        mw = self.mouseWatcherNode
        if self.drag and mw.hasMouse():
            x, y = mw.getMouseX(), mw.getMouseY()
            if self.last:
                dx, dy = x - self.last[0], y - self.last[1]
                if self.drag == "rot":
                    self.heading -= dx * 120
                    self.pitch = max(-85, min(85, self.pitch - dy * 90))
                else:
                    s = self.dist * 0.8
                    right = self.camera.getQuat(self.render).getRight()
                    self.target -= right * dx * s
                    self.target -= Vec3(0, 0, 1) * dy * s
            self.last = (x, y)

        h, p = math.radians(self.heading), math.radians(self.pitch)
        offset = Vec3(math.sin(h) * math.cos(p),
                      -math.cos(h) * math.cos(p),
                      math.sin(p)) * self.dist
        self.camera.setPos(self.target + offset)
        self.camera.lookAt(self.target)
        return task.cont


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Uso: python viewer_avatar.py avatar.glb")
    Viewer(sys.argv[1]).run()