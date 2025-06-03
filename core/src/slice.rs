use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::slice;

use clog_derive::SliceTrait;

use crate::types::*;
use crate::DataBuilder;


#[derive(SliceTrait, Debug)]
pub struct Combined {
    status: Tuple1<u16>,
    method: Tuple1<u32>,
    uri: Tuple1<u32>,
    ip: Tuple2<u32, u32>,
}
pub struct Owned<F: SliceTrait> {
    fields: F,
    len: usize,
    capacity: usize,
    ptr: *mut u8,
}

impl<F: SliceTrait> Owned<F> {
    pub fn with_capacity(n: usize) -> Self {
        let (layout, fields) = F::layout(n);
        //dbg!(n, &fields);

        unsafe {
            let ptr;
            if layout.size() > 0 {
                ptr = System.alloc(layout);
            } else {
                ptr = layout.dangling().as_mut();
            }
            //dbg!(ptr, layout.align());
            Owned { fields, len: 0, capacity: n, ptr }
        }
    }
    pub fn push(&mut self, elem: F::Elem) {
        if self.len >= self.capacity {
            self.reserve(1);
        }
        unsafe {
            assert!(self.len < self.capacity);
            self.fields.write(self.ptr, self.len, elem);
            self.len += 1;
        }
    }
    pub fn get(&self, idx: usize) -> Option<F::Elem> {
        if idx < self.len {
            unsafe {
                // idx < len
                Some(F::get(&self.fields, self.ptr, idx))
            }
        } else {
            None
        }
    }
    pub fn slice<'a>(&'a self) -> F::Slice<'a> {
        unsafe {
            self.fields.slice(self.ptr, self.len)
        }
    }
    pub fn slice_mut<'a>(&'a mut self) -> F::SliceMut<'a> {
        unsafe {
            self.fields.slice_mut(self.ptr, self.len)
        }
    }

    pub fn slice_uninit<'a>(&'a mut self, len: usize) -> F::SliceUninit<'a> {
        unsafe {
            assert!(len <= self.capacity);
            self.fields.slice_uninit(self.ptr, len)
        }
    }
    unsafe fn set_len(&mut self, n: usize) {
        self.len = n;
    }
    pub fn reserve(&mut self, additional: usize) {
        let new_cap = self.capacity.checked_add(additional).expect("overflow");
        assert!(new_cap < isize::MAX as usize);

        let new_cap = new_cap.next_power_of_two();
        assert!(new_cap < isize::MAX as usize);

        let mut new = Self::with_capacity(new_cap);

        unsafe {
            F::copy_slice_uninit(self.slice(), new.slice_uninit(self.len));
            new.len = self.len;
        }

        *self = new;
    }
    pub fn iter(&self) -> impl Iterator<Item=F::Elem> + ExactSizeIterator + DoubleEndedIterator {
        (0..self.len).map(|i| unsafe {
            F::get(&self.fields, self.ptr, i)
        })
    }
    pub fn len(&self) -> usize {
        self.len
    }
}
impl<F: SliceTrait> Drop for Owned<F> {
    fn drop(&mut self) {
        let (layout, fields) = F::layout(self.capacity);
        if layout.size() > 0 {
            unsafe {
                System.dealloc(self.ptr, layout);
            }
        }
    }
}
impl<F: SliceTrait> Clone for Owned<F> {
    fn clone(&self) -> Self {
        let mut new = Self::with_capacity(self.capacity);
        new.len = self.len;
        F::copy_slice(self.slice(), new.slice_mut());
        new
    }
}
impl<F: SliceTrait> Default for Owned<F> {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}
impl<F: SliceTrait> Extend<F::Elem> for Owned<F> {
    fn extend<T: IntoIterator<Item = F::Elem>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        let (min, max) = iter.size_hint();
        let new_len = max.unwrap_or(min) + self.len;
        if new_len > self.capacity {
            self.reserve(new_len - self.capacity);
        }

        for elem in iter {
            self.push(elem);
        }
    }
}
unsafe impl<F: SliceTrait + Send> Send for Owned<F> {}
unsafe impl<F: SliceTrait + Sync> Sync for Owned<F> {}

pub trait SliceTrait: Debug {
    type Slice<'a>;
    type SliceMut<'a>;
    type SliceUninit<'a>;
    type Elem;
    fn layout(capacity: usize) -> (Layout, Self);
    unsafe fn slice<'a>(&self, raw: *mut u8, len: usize) -> Self::Slice<'a>;
    unsafe fn slice_mut<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceMut<'a>;
    unsafe fn slice_uninit<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceUninit<'a>;
    unsafe fn get(&self, raw: *mut u8, idx: usize) -> Self::Elem;
    unsafe fn write(&self, raw: *mut u8, idx: usize, val: Self::Elem);

    fn copy_slice<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceMut<'b>);
    fn copy_slice_uninit<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceUninit<'b>);
}

#[derive(Debug)]
pub struct Tuple1<T> {
    _m: PhantomData<(T,)>
}
impl<T: Copy + Debug> SliceTrait for Tuple1<T>
    where T: 'static
{
    type Slice<'a> = &'a [T];
    type SliceMut<'a> = &'a mut [T];
    type SliceUninit<'a> = &'a mut [MaybeUninit<T>];
    type Elem = T;
    
    #[inline(always)]
    fn layout(capacity: usize) -> (Layout, Self) {
        let layout = Layout::array::<T>(capacity).unwrap();
        (layout, Tuple1 { _m: PhantomData })
    }
    #[inline(always)]
    unsafe fn slice<'a>(&self, raw: *mut u8, len: usize) -> Self::Slice<'a> {
        unsafe {
            slice::from_raw_parts(raw.cast(), len)
        }
    }

    #[inline(always)]
    unsafe fn slice_mut<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceMut<'a> {
        unsafe {
            slice::from_raw_parts_mut(raw.cast(), len)
        }
    }
    #[inline(always)]
    unsafe fn slice_uninit<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceUninit<'a> {
        unsafe {
            slice::from_raw_parts_mut(raw.cast(), len)
        }
    }

    unsafe fn get(&self, raw: *mut u8, idx: usize) -> Self::Elem {
        unsafe {
            raw.cast::<T>().offset(idx as isize).read()
        }    
    }

    unsafe fn write(&self, raw: *mut u8, idx: usize, elem: Self::Elem) {
        unsafe {
            raw.cast::<T>().offset(idx as isize).write(elem)
        }    
    }

    fn copy_slice<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceMut<'b>) {
        to.copy_from_slice(from);
    }
    fn copy_slice_uninit<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceUninit<'b>) {
        unsafe {
            to.copy_from_slice(core::mem::transmute::<&[T], &[MaybeUninit<T>]>(from));
        }
    }
}

#[derive(Debug)]
pub struct Tuple2<T, U> {
    offset_1: usize,
    _m: PhantomData<(T, U)>
}
impl<T: Debug, U: Debug> SliceTrait for Tuple2<T, U>
    where T: Copy + 'static, U: Copy + 'static
{
    type Slice<'a> = (&'a [T], &'a [U]);
    type SliceMut<'a> = (&'a mut [T], &'a mut [U]);
    type SliceUninit<'a> = (&'a mut [MaybeUninit<T>], &'a mut [MaybeUninit<U>]);
    type Elem = (T, U);
    
    #[inline(always)]
    fn layout(capacity: usize) -> (Layout, Self) {
        let layout1 = Layout::array::<T>(capacity).unwrap();
        let layout2 = Layout::array::<U>(capacity).unwrap();

        let (layout, offset_1) = layout1.extend(layout2).unwrap();

        (layout, Tuple2 { offset_1, _m: PhantomData })
    }

    #[inline(always)]
    unsafe fn slice<'a>(&self, raw: *mut u8, len: usize) -> Self::Slice<'a> {
        unsafe {
            (
                slice::from_raw_parts(raw.cast(), len),
                slice::from_raw_parts(raw.offset(self.offset_1 as isize).cast(), len)
            )
        }
    }

    #[inline(always)]
    unsafe fn slice_mut<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceMut<'a> {
        unsafe {
            (
                slice::from_raw_parts_mut(raw.cast(), len),
                slice::from_raw_parts_mut(raw.offset(self.offset_1 as isize).cast(), len)
            )
        }
    }

    #[inline(always)]
    unsafe fn slice_uninit<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceUninit<'a> {
        unsafe {
            (
                slice::from_raw_parts_mut(raw.cast(), len),
                slice::from_raw_parts_mut(raw.offset(self.offset_1 as isize).cast(), len)
            )
        }
    }

    unsafe fn get(&self, raw: *mut u8, idx: usize) -> Self::Elem {
        unsafe {
            (
                raw.cast::<T>().offset(idx as isize).read(),
                raw.offset(self.offset_1 as isize).cast::<U>().offset(idx as isize).read(),
            )
        }    
    }

    unsafe fn write(&self, raw: *mut u8, idx: usize, elem: Self::Elem) {
        let (t, u) = elem;
        unsafe {
            raw.cast::<T>().offset(idx as isize).write(t);
            raw.offset(self.offset_1 as isize).cast::<U>().offset(idx as isize).write(u);
        }    
    }

    fn copy_slice<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceMut<'b>) {
        to.0.copy_from_slice(from.0);
        to.1.copy_from_slice(from.1);
    }
    fn copy_slice_uninit<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceUninit<'b>) {
        unsafe {
            to.0.copy_from_slice(core::mem::transmute::<&[T], &[MaybeUninit<T>]>(from.0));
            to.1.copy_from_slice(core::mem::transmute::<&[U], &[MaybeUninit<U>]>(from.1));
        }
    }
}
