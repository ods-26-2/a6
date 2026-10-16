#!/usr/bin/env python3
"""
fbx_para_glb.py - Converte um FBX (ex.: Y Bot do Mixamo) para GLB.

    pip install bpy
    python fbx_para_glb.py ybot.fbx ybot.glb
"""
import sys

import bpy

if len(sys.argv) < 3:
    sys.exit("Uso: python fbx_para_glb.py entrada.fbx saida.glb")

bpy.ops.wm.read_factory_settings(use_empty=True)
bpy.ops.import_scene.fbx(filepath=sys.argv[1])
bpy.ops.export_scene.gltf(filepath=sys.argv[2], export_format="GLB",
                          export_animations=True, export_apply=False)
print("OK ->", sys.argv[2])